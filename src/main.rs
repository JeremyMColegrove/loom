use log::{debug, error, info};
use loom::runtime::security::{SecurityPolicy, TrustMode};
use serde::Deserialize;
use std::env;
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq)]
enum Mode {
    Lsp,
    Run {
        file_path: String,
        strict: bool,
        policy_path: Option<String>,
        trust_mode_override: Option<TrustMode>,
        require_policy: bool,
    },
}

#[derive(Debug, Deserialize)]
struct LoomPolicyFile {
    version: Option<u32>,
    allow_all: Option<bool>,
    trust_mode: Option<String>,
    read_paths: Option<Vec<String>>,
    write_paths: Option<Vec<String>>,
    import_paths: Option<Vec<String>>,
    watch_paths: Option<Vec<String>>,
    deny_globs: Option<Vec<String>>,
}

fn parse_cli_args(args: &[String]) -> Result<Mode, String> {
    parse_cli_args_with_require_default(args, require_policy_from_env())
}

fn parse_cli_args_with_require_default(
    args: &[String],
    require_policy_default: bool,
) -> Result<Mode, String> {
    let mut strict = true;
    let mut lsp = false;
    let mut file_path: Option<String> = None;
    let mut policy_path: Option<String> = None;
    let mut trust_mode_override: Option<TrustMode> = None;
    let mut require_policy = require_policy_default;

    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--lsp" => lsp = true,
            "--strict" => strict = true,
            "--no-strict" => strict = false,
            "--require-policy" => require_policy = true,
            "--policy" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--policy requires a file path".to_string())?;
                policy_path = Some(value.clone());
            }
            "--trust-mode" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--trust-mode requires 'trusted' or 'restricted'".to_string())?;
                trust_mode_override = Some(parse_trust_mode(value)?);
            }
            s if s.starts_with('-') => return Err(format!("Unknown flag: {}", s)),
            path => {
                if file_path.is_some() {
                    return Err("Only one script file path is supported".to_string());
                }
                file_path = Some(path.to_string());
            }
        }
        i += 1;
    }

    if lsp {
        return Ok(Mode::Lsp);
    }

    let file_path = file_path.ok_or_else(|| "No file specified".to_string())?;
    Ok(Mode::Run {
        file_path,
        strict,
        policy_path,
        trust_mode_override,
        require_policy,
    })
}

fn parse_trust_mode(raw: &str) -> Result<TrustMode, String> {
    match raw.to_ascii_lowercase().as_str() {
        "trusted" => Ok(TrustMode::Trusted),
        "restricted" => Ok(TrustMode::Restricted),
        _ => Err(format!(
            "Invalid trust mode '{}'; expected 'trusted' or 'restricted'",
            raw
        )),
    }
}

fn require_policy_from_env() -> bool {
    env::var("LOOM_REQUIRE_POLICY")
        .ok()
        .map(|v| !matches!(v.to_ascii_lowercase().as_str(), "0" | "false" | "no" | "off"))
        .unwrap_or(true)
}

fn resolve_policy_path(
    script_dir: &Path,
    explicit: Option<&str>,
) -> Result<Option<PathBuf>, String> {
    if let Some(raw) = explicit {
        let path = PathBuf::from(raw);
        if path.exists() {
            return Ok(Some(path));
        }
        return Err(format!("Policy file not found: {}", path.display()));
    }

    let script_candidate = script_dir.join(".loomrc.json");
    if script_candidate.exists() {
        return Ok(Some(script_candidate));
    }

    let cwd_candidate = std::env::current_dir()
        .map_err(|e| format!("Failed to resolve current directory: {}", e))?
        .join(".loomrc.json");
    if cwd_candidate.exists() {
        return Ok(Some(cwd_candidate));
    }
    Ok(None)
}

fn resolve_paths(raw_paths: Option<Vec<String>>, base_dir: &Path) -> Vec<PathBuf> {
    let filesystem_root = current_filesystem_root();

    raw_paths
        .unwrap_or_default()
        .into_iter()
        .map(|p| {
            if p.trim() == "*" {
                return filesystem_root.clone();
            }
            let path = PathBuf::from(p);
            if path.is_absolute() {
                path
            } else {
                base_dir.join(path)
            }
        })
        .collect()
}

fn current_filesystem_root() -> PathBuf {
    std::env::current_dir()
        .ok()
        .and_then(|cwd| cwd.ancestors().last().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from(std::path::MAIN_SEPARATOR.to_string()))
}

fn apply_runtime_policy(
    runtime: &mut loom::runtime::Runtime,
    policy_file_path: Option<&Path>,
    trust_mode_override: Option<TrustMode>,
) -> Result<(), String> {
    let mut trust_mode_from_policy: Option<TrustMode> = None;

    if let Some(path) = policy_file_path {
        let policy_text = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read policy file '{}': {}", path.display(), e))?;
        let raw: LoomPolicyFile = serde_json::from_str(&policy_text)
            .map_err(|e| format!("Invalid policy JSON '{}': {}", path.display(), e))?;

        if let Some(version) = raw.version
            && version != 1
        {
            return Err(format!(
                "Unsupported policy version {} in '{}'; expected version 1",
                version,
                path.display()
            ));
        }

        let LoomPolicyFile {
            version: _,
            allow_all,
            trust_mode,
            read_paths,
            write_paths,
            import_paths,
            watch_paths,
            deny_globs,
        } = raw;

        let policy_dir = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let mut policy = SecurityPolicy::restricted();
        let allow_all_enabled = allow_all.unwrap_or(false);
        let all_paths = vec![current_filesystem_root()];

        if allow_all_enabled || read_paths.is_some() {
            let paths = read_paths
                .map(|paths| resolve_paths(Some(paths), &policy_dir))
                .unwrap_or_else(|| all_paths.clone());
            policy = policy.with_read_paths(paths);
        }
        if allow_all_enabled || write_paths.is_some() {
            let paths = write_paths
                .map(|paths| resolve_paths(Some(paths), &policy_dir))
                .unwrap_or_else(|| all_paths.clone());
            policy = policy.with_write_paths(paths);
        }
        if allow_all_enabled || import_paths.is_some() {
            let paths = import_paths
                .map(|paths| resolve_paths(Some(paths), &policy_dir))
                .unwrap_or_else(|| all_paths.clone());
            policy = policy.with_import_paths(paths);
        }
        if allow_all_enabled || watch_paths.is_some() {
            let paths = watch_paths
                .map(|paths| resolve_paths(Some(paths), &policy_dir))
                .unwrap_or_else(|| all_paths.clone());
            policy = policy.with_watch_paths(paths);
        }
        if let Some(deny_globs) = deny_globs {
            policy = policy
                .with_deny_globs(deny_globs)
                .map_err(|e| e.to_string())?;
        }
        runtime
            .set_security_policy(policy)
            .map_err(|e| format!("Invalid policy '{}': {}", path.display(), e))?;

        if let Some(mode) = trust_mode {
            trust_mode_from_policy = Some(parse_trust_mode(&mode)?);
        }
    }

    if let Some(mode) = trust_mode_override.or(trust_mode_from_policy) {
        runtime.set_trust_mode(mode);
    }
    Ok(())
}

fn init_logging() {
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().filter_or("LOOM_LOG", "warn"));
    builder.format_timestamp_secs();
    builder.format_target(false);
    let _ = builder.try_init();
}

#[tokio::main]
async fn main() -> Result<(), String> {
    init_logging();
    let args: Vec<String> = env::args().collect();

    match parse_cli_args(&args) {
        Ok(Mode::Lsp) => {
            info!("starting LSP server");
            loom::lsp::server::run_server().await;
            Ok(())
        }
        Ok(Mode::Run {
            file_path,
            strict,
            policy_path,
            trust_mode_override,
            require_policy,
        }) => {
            let source = tokio::fs::read_to_string(&file_path)
                .await
                .map_err(|e| format!("Failed to read '{}': {}", file_path, e))?;
            debug!("loaded script from {}", file_path);

            match loom::parser::parse(&source) {
                Ok(program) => {
                    let issues = loom::validator::validate_program(&program);
                    let has_errors = issues
                        .iter()
                        .any(|i| matches!(i.severity, loom::validator::ValidationSeverity::Error));
                    let has_warnings = issues.iter().any(|i| {
                        matches!(i.severity, loom::validator::ValidationSeverity::Warning)
                    });
                    let must_fail = has_errors || (strict && has_warnings);
                    if must_fail {
                        eprintln!("Validation errors:");
                        for issue in &issues {
                            let level = match issue.severity {
                                loom::validator::ValidationSeverity::Error => "error",
                                loom::validator::ValidationSeverity::Warning => "warning",
                            };
                            eprintln!(
                                "  {} at {}:{} - {}",
                                level, issue.span.start.line, issue.span.start.col, issue.message
                            );
                        }
                        return Err(format!("{} validation issue(s) found", issues.len()));
                    }

                    debug!(
                        "parsed script successfully ({} statements)",
                        program.statements.len()
                    );

                    // Determine script directory for import resolution
                    let script_dir = std::path::Path::new(&file_path)
                        .parent()
                        .map(|p| p.to_string_lossy().to_string())
                        .unwrap_or_else(|| ".".to_string());

                    let mut runtime = loom::runtime::Runtime::new().with_script_dir(&script_dir);
                    let policy_file =
                        resolve_policy_path(Path::new(&script_dir), policy_path.as_deref())?;
                    if require_policy && policy_file.is_none() {
                        return Err(format!(
                            "Policy file required but not found. Provide --policy <path> or add '.loomrc.json' next to '{}'.",
                            file_path
                        ));
                    }
                    apply_runtime_policy(
                        &mut runtime,
                        policy_file.as_deref(),
                        trust_mode_override,
                    )?;
                    let shutdown_trigger = runtime.shutdown_trigger();

                    let exec = runtime.execute(&program);
                    tokio::pin!(exec);
                    tokio::select! {
                        result = &mut exec => {
                            if let Err(e) = result {
                                error!("runtime error while executing '{}': {}", file_path, e);
                                eprintln!("Runtime error: {}", e);
                                return Err(e);
                            }
                        }
                        signal = tokio::signal::ctrl_c() => {
                            match signal {
                                Ok(()) => {
                                    info!("shutdown requested; stopping active watch flows");
                                    let _ = shutdown_trigger.send(true);
                                    if let Err(e) = exec.await {
                                        error!("runtime error during shutdown: {}", e);
                                        eprintln!("Runtime error: {}", e);
                                        return Err(e);
                                    }
                                }
                                Err(e) => {
                                    let msg = format!("Failed to listen for Ctrl+C: {}", e);
                                    error!("{}", msg);
                                    eprintln!("{}", msg);
                                    return Err(msg);
                                }
                            }
                        }
                    }

                    Ok(())
                }
                Err(errors) => {
                    eprintln!("Parse errors:");
                    for err in &errors {
                        eprintln!("  Line {}:{} - {}", err.line, err.col, err.message);
                    }
                    Err(format!("{} error(s) found", errors.len()))
                }
            }
        }
        Err(err) => {
            eprintln!(
                "Usage: loom [--strict|--no-strict] [--policy <file>] [--trust-mode trusted|restricted] [--require-policy] <file.loom> or loom --lsp (policy required by default; set LOOM_REQUIRE_POLICY=0 to disable)"
            );
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Mode, apply_runtime_policy, current_filesystem_root, parse_cli_args_with_require_default,
        resolve_paths,
    };
    use loom::runtime::security::TrustMode;
    use tempfile::tempdir;

    #[test]
    fn wildcard_paths_resolve_to_filesystem_root() {
        let base = std::env::temp_dir();
        let resolved = resolve_paths(Some(vec!["*".to_string()]), &base);
        let expected_root = current_filesystem_root();
        assert_eq!(resolved, vec![expected_root]);
    }

    #[test]
    fn strict_mode_is_default() {
        let args = vec!["loom".to_string(), "script.loom".to_string()];
        let parsed = parse_cli_args_with_require_default(&args, true).expect("args should parse");
        assert_eq!(
            parsed,
            Mode::Run {
                file_path: "script.loom".to_string(),
                strict: true,
                policy_path: None,
                trust_mode_override: None,
                require_policy: true,
            }
        );
    }

    #[test]
    fn strict_mode_can_be_disabled_explicitly() {
        let args = vec![
            "loom".to_string(),
            "--no-strict".to_string(),
            "script.loom".to_string(),
        ];
        let parsed = parse_cli_args_with_require_default(&args, true).expect("args should parse");
        assert_eq!(
            parsed,
            Mode::Run {
                file_path: "script.loom".to_string(),
                strict: false,
                policy_path: None,
                trust_mode_override: None,
                require_policy: true,
            }
        );
    }

    #[test]
    fn policy_and_trust_mode_flags_parse() {
        let args = vec![
            "loom".to_string(),
            "--policy".to_string(),
            ".loomrc.json".to_string(),
            "--trust-mode".to_string(),
            "restricted".to_string(),
            "--require-policy".to_string(),
            "script.loom".to_string(),
        ];
        let parsed = parse_cli_args_with_require_default(&args, false).expect("args should parse");
        assert_eq!(
            parsed,
            Mode::Run {
                file_path: "script.loom".to_string(),
                strict: true,
                policy_path: Some(".loomrc.json".to_string()),
                trust_mode_override: Some(TrustMode::Restricted),
                require_policy: true,
            }
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn allow_all_policy_allows_imports_without_explicit_path_lists() {
        let script_dir = tempdir().expect("script dir");
        let external_dir = tempdir().expect("external dir");

        let module_path = external_dir.path().join("logic.loom");
        std::fs::write(&module_path, "value() => 1").expect("write module");

        let policy_path = script_dir.path().join(".loomrc.json");
        std::fs::write(
            &policy_path,
            r#"{"version":1,"trust_mode":"trusted","allow_all":true}"#,
        )
        .expect("write policy");

        let source = format!("@import \"{}\" as lib", module_path.to_string_lossy());
        let program = loom::parser::parse(&source).expect("parse program");

        let mut runtime = loom::runtime::Runtime::new()
            .with_script_dir(script_dir.path().to_str().expect("script dir path"));
        apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
            .expect("apply policy should succeed");

        runtime.execute(&program).await.expect("import should succeed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wildcard_path_lists_allow_external_imports() {
        let script_dir = tempdir().expect("script dir");
        let external_dir = tempdir().expect("external dir");

        let module_path = external_dir.path().join("logic.loom");
        std::fs::write(&module_path, "value() => 1").expect("write module");

        let policy_path = script_dir.path().join(".loomrc.json");
        std::fs::write(
            &policy_path,
            r#"{
                "version":1,
                "trust_mode":"trusted",
                "allow_all":false,
                "read_paths":["*"],
                "import_paths":["*"]
            }"#,
        )
        .expect("write policy");

        let source = format!("@import \"{}\" as lib", module_path.to_string_lossy());
        let program = loom::parser::parse(&source).expect("parse program");

        let mut runtime = loom::runtime::Runtime::new()
            .with_script_dir(script_dir.path().to_str().expect("script dir path"));
        apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
            .expect("apply policy should succeed");

        runtime.execute(&program).await.expect("import should succeed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn allow_all_with_restrictive_import_paths_only_limits_imports() {
        let script_dir = tempdir().expect("script dir");
        let external_dir = tempdir().expect("external dir");

        let module_path = external_dir.path().join("logic.loom");
        std::fs::write(&module_path, "value() => 1").expect("write module");
        let data_path = external_dir.path().join("data.txt");
        std::fs::write(&data_path, "ok").expect("write data");

        let policy_path = script_dir.path().join(".loomrc.json");
        std::fs::write(
            &policy_path,
            format!(
                r#"{{
                    "version":1,
                    "trust_mode":"trusted",
                    "allow_all":true,
                    "import_paths":["{}"]
                }}"#,
                script_dir.path().to_string_lossy()
            ),
        )
        .expect("write policy");

        let read_program = loom::parser::parse(&format!("\"{}\" >> @read", data_path.to_string_lossy()))
            .expect("parse read program");
        let import_program =
            loom::parser::parse(&format!("@import \"{}\" as lib", module_path.to_string_lossy()))
                .expect("parse import program");

        let mut runtime = loom::runtime::Runtime::new()
            .with_script_dir(script_dir.path().to_str().expect("script dir path"));
        apply_runtime_policy(&mut runtime, Some(policy_path.as_path()), None)
            .expect("apply policy should succeed");

        runtime
            .execute(&read_program)
            .await
            .expect("reads should remain allow-all");
        let err = runtime
            .execute(&import_program)
            .await
            .expect_err("import should be restricted");
        assert!(err.contains("Unauthorized Import"));
    }
}
