use log::{debug, error, info};
use std::env;

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

    if args.contains(&"--lsp".to_string()) {
        info!("starting LSP server");
        loom::lsp::server::run_server().await;
        Ok(())
    } else if let Some(file_path) = args.get(1) {
        let source = tokio::fs::read_to_string(file_path)
            .await
            .map_err(|e| format!("Failed to read '{}': {}", file_path, e))?;
        debug!("loaded script from {}", file_path);

        match loom::parser::parse(&source) {
            Ok(program) => {
                debug!(
                    "parsed script successfully ({} statements)",
                    program.statements.len()
                );

                // Determine script directory for import resolution
                let script_dir = std::path::Path::new(file_path)
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|| ".".to_string());

                let mut runtime = loom::runtime::Runtime::new().with_script_dir(&script_dir);
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
    } else {
        eprintln!("Usage: loom <file.loom> or loom --lsp");
        Err("No file specified".to_string())
    }
}
