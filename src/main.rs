use std::env;

#[tokio::main]
async fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    
    if args.contains(&"--lsp".to_string()) {
        loom::lsp::server::run_server().await;
        Ok(())
    } else if let Some(file_path) = args.get(1) {
        let source = tokio::fs::read_to_string(file_path).await.map_err(|e| e.to_string())?;
        
        println!("🧶 Loom v0.1.0");
        println!("📄 Loading: {}", file_path);
        println!();
        
        match loom::parser::parse(&source) {
            Ok(program) => {
                println!("✅ Parsed successfully ({} statements)", program.statements.len());
                println!();
                
                // Determine script directory for import resolution
                let script_dir = std::path::Path::new(file_path)
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|| ".".to_string());
                
                let mut runtime = loom::runtime::Runtime::new()
                    .with_script_dir(&script_dir);
                
                if let Err(e) = runtime.execute(&program).await {
                    println!("❌ Runtime error: {}", e);
                    return Err(e);
                }
                
                println!();
                println!("✨ Done.");
                Ok(())
            }
            Err(errors) => {
                println!("❌ Parse errors:");
                for err in &errors {
                    println!("   Line {}:{} — {}", err.line, err.col, err.message);
                }
                Err(format!("{} error(s) found", errors.len()))
            }
        }
    } else {
        eprintln!("Usage: loom <file.loom> or loom --lsp");
        Err("No file specified".to_string())
    }
}