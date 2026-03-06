use log::{debug, error, info};
use loom::policy::{apply_runtime_policy, parse_trust_mode};
use loom::runtime::security::TrustMode;
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

fn require_policy_from_env() -> bool {
    env::var("LOOM_REQUIRE_POLICY")
        .ok()
        .map(|v| {
            !matches!(
                v.to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            )
        })
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

fn init_logging() {
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().filter_or("LOOM_LOG", "warn"));
    builder.format_timestamp_secs();
    builder.format_target(false);
    let _ = builder.try_init();
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        if !err.is_empty() {
            eprintln!("{}", err);
        }
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
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

                    // Determine script directory for import and secret resolution.
                    // Prefer a canonicalized script path so lookup is stable across cwd changes.
                    let resolved_script_path = std::fs::canonicalize(&file_path)
                        .unwrap_or_else(|_| std::path::PathBuf::from(&file_path));
                    let script_dir = resolved_script_path
                        .parent()
                        .map(|p| p.to_string_lossy().to_string())
                        .or_else(|| {
                            std::env::current_dir()
                                .ok()
                                .map(|p| p.to_string_lossy().to_string())
                        })
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
                                return Err(String::new());
                            }
                        }
                        signal = tokio::signal::ctrl_c() => {
                            match signal {
                                Ok(()) => {
                                    info!("shutdown requested; stopping active watch flows");
                                    let _ = shutdown_trigger.send(true);
                                    if let Err(e) = exec.await {
                                        error!("runtime error during shutdown: {}", e);
                                        return Err(String::new());
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
#[path = "../tests/unit/main_tests.rs"]
mod main_tests;
