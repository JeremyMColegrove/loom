use crate::ast::*;
use crate::builtin_spec::{
    DIRECTIVE_ATOMIC, DIRECTIVE_CSV_PARSE, DIRECTIVE_FILTER, DIRECTIVE_LINES, DIRECTIVE_MAP,
    DIRECTIVE_READ, DIRECTIVE_SECRET, DIRECTIVE_WRITE,
};
use crate::runtime::Runtime;
use crate::runtime::builtins::{extract_read_path, normalize_csv_for_parsing};
use crate::runtime::env::Value;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use csv::{ReaderBuilder, Trim};
use log::debug;
use std::collections::HashMap;

impl Runtime {
    pub(crate) fn eval_directive<'a>(
        &'a mut self,
        directive: &'a DirectiveFlow,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Value>> + 'a>> {
        self.eval_directive_with_pipe(directive, Value::Null)
    }

    pub(crate) fn eval_directive_with_pipe<'a>(
        &'a mut self,
        directive: &'a DirectiveFlow,
        pipe_val: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Value>> + 'a>> {
        Box::pin(async move {
            let (args, named_args) = self
                .eval_call_arguments(&directive.arguments, &directive.named_arguments)
                .await?;

            debug!(
                "evaluating directive: @{}{}",
                directive.name,
                if args.is_empty() {
                    String::new()
                } else {
                    format!(
                        "({})",
                        args.iter()
                            .map(|a| a.as_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            );

            if directive.name == DIRECTIVE_ATOMIC {
                if !self.atomic_active {
                    self.begin_atomic()?;
                }
                if let Some(alias) = &directive.alias {
                    self.env.set(alias, pipe_val.clone());
                }
                return Ok(pipe_val);
            }

            if directive.name == DIRECTIVE_FILTER {
                let list_input = match &pipe_val {
                    Value::Record(map) => map.get("rows").cloned().unwrap_or(pipe_val.clone()),
                    _ => pipe_val.clone(),
                };
                let mut call_args = vec![list_input];
                call_args.extend(args);
                let result = self.call_filter(call_args).await?;
                if let Some(alias) = &directive.alias {
                    self.env.set(alias, result.clone());
                }
                return Ok(result);
            }

            if directive.name == DIRECTIVE_MAP {
                let list_input = match &pipe_val {
                    Value::Record(map) => map.get("rows").cloned().unwrap_or(pipe_val.clone()),
                    _ => pipe_val.clone(),
                };
                let mut call_args = vec![list_input];
                call_args.extend(args);
                let result = self.call_map(call_args).await?;
                if let Some(alias) = &directive.alias {
                    self.env.set(alias, result.clone());
                }
                return Ok(result);
            }

            if directive.name == DIRECTIVE_READ {
                let source = named_args
                    .get("path")
                    .cloned()
                    .or_else(|| args.first().cloned())
                    .unwrap_or(pipe_val);
                let path = extract_read_path(&source).ok_or_else(|| {
                    RuntimeError::message(
                        "@read expects a path, event, or variable containing a path",
                    )
                })?;
                return Ok(Value::String(self.read_text_path(&path)?));
            }

            if directive.name == DIRECTIVE_WRITE {
                let path = named_args
                    .get("path")
                    .map(|v| v.as_string())
                    .or_else(|| args.first().map(|v| v.as_string()))
                    .unwrap_or_else(|| "output.txt".to_string());
                self.write_path(&path, &pipe_val.as_string())?;
                return Ok(Value::String(path));
            }

            if directive.name == DIRECTIVE_LINES {
                let source = named_args
                    .get("path")
                    .cloned()
                    .or_else(|| args.first().cloned())
                    .unwrap_or(pipe_val);
                let source_path = source
                    .as_path()
                    .ok_or_else(|| RuntimeError::message("@lines expects a file path source"))?
                    .to_string();
                let text = self.read_text_path(&source_path)?;
                let lines = text
                    .lines()
                    .map(|line| Value::String(line.to_string()))
                    .collect();
                return Ok(Value::List(lines));
            }

            if directive.name == DIRECTIVE_SECRET {
                let resolved = self.resolve_secret_from_call_args(&args, &named_args)?;
                let value = Value::String(resolved);
                if let Some(alias) = &directive.alias {
                    self.env.set(alias, value.clone());
                }
                return Ok(value);
            }

            if directive.name == DIRECTIVE_CSV_PARSE {
                return self.parse_csv_from_pipe(pipe_val);
            }

            if directive.name == "http.post" {
                return self.http_post(args, named_args, pipe_val).await;
            }

            if let Some(unknown_name) = directive.name.strip_prefix("http.") {
                let _ = unknown_name;
                return Err(RuntimeError::message(format!(
                    "Unknown directive: @{}",
                    directive.name
                )));
            }

            let result = if let Some(handler) = self.builtins.get_directive(&directive.name) {
                tokio::task::spawn_blocking(move || handler(args, pipe_val))
                    .await
                    .map_err(|e| RuntimeError::message(format!("Directive task failed: {}", e)))??
            } else {
                return Err(RuntimeError::message(format!(
                    "Unknown directive: @{}",
                    directive.name
                )));
            };

            // Bind the alias if present
            if let Some(alias) = &directive.alias {
                self.env.set(alias, result.clone());
            }

            Ok(result)
        })
    }

    fn parse_csv_from_pipe(&mut self, pipe_val: Value) -> RuntimeResult<Value> {
        let (source, csv_text) = match &pipe_val {
            Value::Path(path) => (path.clone(), self.read_text_path(path)?),
            Value::String(text) => (text.clone(), text.clone()),
            Value::List(items) => {
                let text = items
                    .iter()
                    .map(|v| v.as_string())
                    .collect::<Vec<_>>()
                    .join("");
                ("list".to_string(), text)
            }
            Value::Record(_) => {
                if let Some(path) = pipe_val.as_path() {
                    (path.to_string(), self.read_text_path(path)?)
                } else {
                    return Err(RuntimeError::message(
                        "@csv.parse received a record without a file path",
                    ));
                }
            }
            other => {
                let text = other.as_string();
                (text.clone(), text.clone())
            }
        };

        let normalized = normalize_csv_for_parsing(&csv_text);
        let mut reader = ReaderBuilder::new()
            .trim(Trim::All)
            .from_reader(normalized.as_bytes());

        let headers = reader
            .headers()
            .map_err(|e| {
                RuntimeError::message(format!(
                    "Failed to parse CSV headers from '{}': {}",
                    source, e
                ))
            })?
            .iter()
            .map(|h| h.to_string())
            .collect::<Vec<_>>();

        let mut rows = Vec::new();
        for (row_index, row) in reader.records().enumerate() {
            self.enforce_row_limit(row_index + 1, &source)?;
            let record = row.map_err(|e| {
                RuntimeError::message(format!("Failed to parse CSV row from '{}': {}", source, e))
            })?;
            if record.len() != headers.len() {
                return Err(RuntimeError::message(format!(
                    "CSV row {} has {} fields, but header has {} fields",
                    row_index + 2,
                    record.len(),
                    headers.len()
                )));
            }
            let mut mapped = HashMap::new();
            for (idx, value) in record.iter().enumerate() {
                let key = headers
                    .get(idx)
                    .cloned()
                    .unwrap_or_else(|| format!("col{}", idx + 1));
                mapped.insert(key, Value::String(value.to_string()));
            }
            rows.push(Value::Record(mapped));
        }

        let mut record = HashMap::new();
        record.insert("source".to_string(), Value::String(source));
        record.insert("valid".to_string(), Value::Boolean(true));
        record.insert(
            "headers".to_string(),
            Value::List(headers.iter().cloned().map(Value::String).collect()),
        );
        record.insert("rows".to_string(), Value::List(rows));
        Ok(Value::Record(record))
    }
}
