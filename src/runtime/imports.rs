use crate::ast::*;
use crate::runtime::Runtime;
use crate::runtime::env::Value;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use log::debug;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

impl Runtime {
    pub(crate) fn execute_import<'a>(
        &'a mut self,
        import: &'a ImportStmt,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<()>> + 'a>> {
        Box::pin(async move {
            let path_str = &import.path;

            if path_str.starts_with("std") {
                return self.register_std_import(import);
            }

            let import_path = self.resolve_import_path(path_str)?;
            let import_key = import_path.to_string_lossy().to_string();

            let cached_exports = {
                self.module_loader
                    .read()
                    .await
                    .cache
                    .get(&import_key)
                    .cloned()
            };
            if let Some(exports) = cached_exports {
                self.bind_module_alias(import, exports);
                return Ok(());
            }

            {
                let mut loader = self.module_loader.write().await;
                if loader.loading.contains(&import_key) {
                    return Err(RuntimeError::message(format!(
                        "Cyclic import detected for '{}'",
                        import.path
                    )));
                }
                loader.loading.insert(import_key.clone());
            }

            let content = match self.read_text_path(&import_key) {
                Ok(content) => content,
                Err(err) => {
                    self.module_loader.write().await.loading.remove(&import_key);
                    return Err(err);
                }
            };

            let parsed = match crate::parser::parse(&content) {
                Ok(parsed) => parsed,
                Err(errors) => {
                    self.module_loader.write().await.loading.remove(&import_key);
                    let msgs: Vec<String> = errors
                        .iter()
                        .map(|e| format!("  Line {}:{} — {}", e.line, e.col, e.message))
                        .collect();
                    return Err(RuntimeError::message(format!(
                        "Parse errors in '{}':\n{}",
                        import.path,
                        msgs.join("\n")
                    )));
                }
            };

            // Execute in an isolated runtime
            let mut isolated_runtime = Runtime::new();
            let parent_dir = import_path
                .parent()
                .and_then(|p| p.to_str())
                .unwrap_or_default()
                .to_string();
            if !parent_dir.is_empty() {
                isolated_runtime = isolated_runtime.with_script_dir(&parent_dir);
            }
            isolated_runtime.set_security_policy(self.security_policy.clone())?;
            isolated_runtime.set_trust_mode(self.trust_mode);
            isolated_runtime.module_loader = self.module_loader.clone();

            let execute_result = isolated_runtime.execute(&parsed).await;
            if let Err(e) = execute_result {
                self.module_loader.write().await.loading.remove(&import_key);
                return Err(RuntimeError::message(e));
            }

            // Extract the global namespace of the module
            let exports = isolated_runtime.env.extract_globals();
            {
                let mut loader = self.module_loader.write().await;
                loader.loading.remove(&import_key);
                loader.cache.insert(import_key.clone(), exports.clone());
            }
            self.bind_module_alias(import, exports);

            debug!(
                "imported module '{}' as '{}'",
                path_str,
                import.alias.clone().unwrap_or_else(|| path_str.clone())
            );
            Ok(())
        })
    }

    fn resolve_import_path(&mut self, path_str: &str) -> RuntimeResult<PathBuf> {
        let base_dir = self.script_dir.clone().unwrap_or_default();

        // Try the path as-is first, then with dots replaced by path separators.
        let candidates = vec![path_str.to_string(), path_str.replace('.', "/")];

        let mut import_path = None;
        for candidate in &candidates {
            let mut p = PathBuf::from(&base_dir);
            p.push(candidate);
            if p.extension().is_none() {
                p.set_extension("loom");
            }
            match self.authorize_import_path(&p.to_string_lossy()) {
                Ok(authorized) => {
                    import_path = Some(authorized);
                    break;
                }
                Err(err) => {
                    if err.is_security_denial() {
                        return Err(err);
                    }
                }
            }
        }

        let import_path = import_path.ok_or_else(|| {
            RuntimeError::message(format!("Import module not found: {}", path_str))
        })?;
        Ok(import_path)
    }

    fn bind_module_alias(&mut self, import: &ImportStmt, exports: HashMap<String, Value>) {
        if let Some(alias) = &import.alias {
            self.env.set(alias, Value::Record(exports));
        }
    }

    pub(crate) fn register_std_import(&mut self, import: &ImportStmt) -> RuntimeResult<()> {
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
                    body: FlowOrBranch::Flow(Box::new(PipeFlow {
                        comments: vec![],
                        source: Source::Expression(Expression::Identifier("input".to_string())),
                        operations: vec![(
                            PipeOp::Safe,
                            Destination::Directive(DirectiveFlow {
                                name: "csv.parse".to_string(),
                                arguments: vec![],
                                named_arguments: vec![],
                                alias: None,
                                span: Span::default(),
                            }),
                        )],
                        on_fail: None,
                        span: Span::default(),
                    })),
                    span: Span::default(),
                };
                exports.insert(
                    "parse".to_string(),
                    Value::Function(Arc::new(parse_func.clone())),
                );

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
                    body: FlowOrBranch::Flow(Box::new(PipeFlow {
                        comments: vec![],
                        source: Source::Expression(Expression::Identifier("input".to_string())),
                        operations: vec![(
                            PipeOp::Safe,
                            Destination::FunctionCall(FunctionCall {
                                name: "print".to_string(),
                                arguments: vec![],
                                named_arguments: vec![],
                                alias: None,
                                span: Span::default(),
                            }),
                        )],
                        on_fail: None,
                        span: Span::default(),
                    })),
                    span: Span::default(),
                });
                self.callable_sinks.insert(sink_name.clone());
                let label = import.alias.clone().unwrap_or_else(|| path_str.clone());
                debug!("imported standard module: {}", label);
                Ok(())
            }
            "http" => {
                self.std_modules.insert("http".to_string());
                let label = import.alias.clone().unwrap_or_else(|| path_str.clone());
                debug!("imported standard module: {}", label);
                Ok(())
            }
            _ => Err(RuntimeError::message(format!(
                "Unknown standard module: '{}'",
                path_str
            ))),
        }
    }
}
