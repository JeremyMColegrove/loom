use crate::ast::*;
use crate::runtime::Runtime;
use crate::runtime::env::Value;
use log::debug;

impl Runtime {
    pub(crate) fn execute_import<'a>(
        &'a mut self,
        import: &'a ImportStmt,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + 'a>> {
        Box::pin(async move {
            let path_str = &import.path;

            if path_str.starts_with("std") {
                return self.register_std_import(import);
            }

            let base_dir = self.script_dir.clone().unwrap_or_default();

            // Try the path as-is first, then with dots replaced by path separators
            let candidates = vec![path_str.clone(), path_str.replace('.', "/")];

            let mut import_path = None;
            for candidate in &candidates {
                let mut p = std::path::PathBuf::from(&base_dir);
                p.push(candidate);
                if p.extension().is_none() {
                    p.set_extension("loom");
                }
                if p.exists() {
                    import_path = Some(p);
                    break;
                }
            }

            let import_path =
                import_path.ok_or_else(|| format!("Import module not found: {}", path_str))?;

            let content = std::fs::read_to_string(&import_path)
                .map_err(|e| format!("Failed to read module: {}", e))?;

            let parsed = crate::parser::parse(&content).map_err(|errors| {
                let msgs: Vec<String> = errors
                    .iter()
                    .map(|e| format!("  Line {}:{} — {}", e.line, e.col, e.message))
                    .collect();
                format!("Parse errors in '{}':\n{}", import.path, msgs.join("\n"))
            })?;

            // Execute in an isolated runtime
            let mut isolated_runtime = Runtime::new();
            if let Some(dir) = &self.script_dir {
                isolated_runtime = isolated_runtime.with_script_dir(dir);
            }
            isolated_runtime.execute(&parsed).await?;

            // Extract the global namespace of the module
            let exports = isolated_runtime.env.extract_globals();
            if let Some(alias) = &import.alias {
                self.env.set(alias, Value::Record(exports));
            }

            debug!(
                "imported module '{}' as '{}'",
                path_str,
                import.alias.clone().unwrap_or_else(|| path_str.clone())
            );
            Ok(())
        })
    }

    pub(crate) fn register_std_import(&mut self, import: &ImportStmt) -> Result<(), String> {
        let path_str = &import.path;
        let path = path_str.trim_end_matches(".loom").replace('/', ".");
        let module = path.strip_prefix("std.").unwrap_or(&path);

        match module {
            "csv" => {
                let base_name = import.alias.clone().unwrap_or_default();
                let parse_name = if base_name.is_empty() {
                    "parse".to_string()
                } else {
                    format!("{}.parse", base_name)
                };

                let mut exports = std::collections::HashMap::new();
                let parse_func = FunctionDef {
                    comments: vec![],
                    name: parse_name.clone(),
                    parameters: vec!["input".to_string()],
                    body: FlowOrBranch::Flow(PipeFlow {
                        comments: vec![],
                        source: Source::Expression(Expression::Identifier("input".to_string())),
                        operations: vec![(
                            PipeOp::Safe,
                            Destination::Directive(DirectiveFlow {
                                name: "csv.parse".to_string(),
                                arguments: vec![],
                                alias: None,
                                span: Span::default(),
                            }),
                        )],
                        on_fail: None,
                        span: Span::default(),
                    }),
                    span: Span::default(),
                };
                exports.insert("parse".to_string(), Value::Function(parse_func.clone()));

                if let Some(alias) = &import.alias {
                    self.env.set(alias, Value::Record(exports));
                } else {
                    self.env.register_function(parse_func);
                }
                self.callable_sinks.insert(parse_name.clone());

                let label = import.alias.clone().unwrap_or_else(|| path_str.clone());
                debug!("imported standard module: {}", label);
                Ok(())
            }
            "out" => {
                let sink_name = import.alias.clone().unwrap_or_else(|| "out".to_string());
                self.env.register_function(FunctionDef {
                    comments: vec![],
                    name: sink_name.clone(),
                    parameters: vec!["input".to_string()],
                    body: FlowOrBranch::Flow(PipeFlow {
                        comments: vec![],
                        source: Source::Expression(Expression::Identifier("input".to_string())),
                        operations: vec![(
                            PipeOp::Safe,
                            Destination::FunctionCall(FunctionCall {
                                name: "print".to_string(),
                                arguments: vec![],
                                alias: None,
                                span: Span::default(),
                            }),
                        )],
                        on_fail: None,
                        span: Span::default(),
                    }),
                    span: Span::default(),
                });
                self.callable_sinks.insert(sink_name.clone());
                let label = import.alias.clone().unwrap_or_else(|| path_str.clone());
                debug!("imported standard module: {}", label);
                Ok(())
            }
            _ => Err(format!("Unknown standard module: '{}'", path_str)),
        }
    }
}
