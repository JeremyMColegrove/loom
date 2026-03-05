use crate::ast::*;
use crate::runtime::env::Value;
use crate::runtime::Runtime;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use std::path::Path;

impl Runtime {
    pub(crate) fn execute_watch_flow<'a>(&'a mut self, flow: &'a PipeFlow, watch: &'a DirectiveFlow) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            let watch_path_raw = if let Some(first) = watch.arguments.first() {
                self.eval_expression(first).await?
                    .as_path()
                    .ok_or_else(|| "@watch(path) requires a path".to_string())?
                    .to_string()
            } else {
                ".".to_string()
            };
            let watch_path = self.absolutize_watch_path(&watch_path_raw)?;

            let mut known = self.scan_watch_path(&watch_path)?;
            loop {
                sleep(Duration::from_millis(500)).await;
                let snapshot = self.scan_watch_path(&watch_path)?;

                for (path, modified) in &snapshot {
                    let event_type = if known.contains_key(path) {
                        if known.get(path) == Some(modified) {
                            continue;
                        }
                        "modified"
                    } else {
                        "created"
                    };
                    let event = self.make_watch_event(path, event_type)?;
                    let _ = self.run_watch_event(flow, watch, event).await?;
                }

                for path in known.keys() {
                    if !snapshot.contains_key(path) {
                        let event = self.make_watch_event(path, "deleted")?;
                        let _ = self.run_watch_event(flow, watch, event).await?;
                    }
                }

                known = snapshot;
            }
        })
    }

    pub(crate) fn absolutize_watch_path(&self, watch_path: &str) -> Result<String, String> {
        let mut path = std::path::PathBuf::from(watch_path);
        if !path.is_absolute() {
            if let Some(dir) = &self.script_dir {
                path = std::path::PathBuf::from(dir).join(path);
            }
        }
        std::fs::canonicalize(&path)
            .map(|p| p.to_string_lossy().to_string())
            .map_err(|e| format!("Failed to resolve watch path '{}': {}", path.display(), e))
    }

    pub(crate) fn run_watch_event<'a>(&'a mut self, flow: &'a PipeFlow, watch: &'a DirectiveFlow, event: Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            if let Some(alias) = &watch.alias {
                self.env.set(alias, event.clone());
            }
            self.run_flow_operations(flow, event, false).await
        })
    }

    pub(crate) fn scan_watch_path(&self, watch_path: &str) -> Result<std::collections::HashMap<String, SystemTime>, String> {
        let path = Path::new(watch_path);
        let mut map = std::collections::HashMap::new();
        if path.is_file() {
            let meta = std::fs::metadata(path)
                .map_err(|e| format!("Failed to stat watch target '{}': {}", watch_path, e))?;
            let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            map.insert(path.to_string_lossy().to_string(), modified);
            return Ok(map);
        }
        if !path.exists() {
            return Err(format!("Watch path does not exist: '{}'", watch_path));
        }

        let entries = std::fs::read_dir(path)
            .map_err(|e| format!("Failed to read watch directory '{}': {}", watch_path, e))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("Failed to read watch directory entry: {}", e))?;
            let entry_path = entry.path();
            if entry_path.is_file() {
                let meta = entry.metadata()
                    .map_err(|e| format!("Failed to stat '{}': {}", entry_path.to_string_lossy(), e))?;
                let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                map.insert(entry_path.to_string_lossy().to_string(), modified);
            }
        }
        Ok(map)
    }

    pub(crate) fn make_watch_event(&self, file_path: &str, event_type: &str) -> Result<Value, String> {
        let metadata = std::fs::metadata(file_path).ok();
        let modified = metadata
            .as_ref()
            .and_then(|m| m.modified().ok())
            .unwrap_or(SystemTime::now());
        let secs = modified.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs();
        let approx_year = 1970 + (secs / (365 * 24 * 60 * 60)) as i64;

        let mut created_at = std::collections::HashMap::new();
        created_at.insert("year".to_string(), Value::Number(approx_year as f64));

        let mut file_record = std::collections::HashMap::new();
        let path = Path::new(file_path);
        file_record.insert("path".to_string(), Value::String(file_path.to_string()));
        file_record.insert("name".to_string(), Value::String(
            path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string()
        ));
        file_record.insert("ext".to_string(), Value::String(
            path.extension().and_then(|e| e.to_str()).unwrap_or("").to_string()
        ));
        file_record.insert("created_at".to_string(), Value::Record(created_at));

        let mut event = std::collections::HashMap::new();
        event.insert("file".to_string(), Value::Record(file_record));
        event.insert("path".to_string(), Value::String(file_path.to_string()));
        event.insert("type".to_string(), Value::String(event_type.to_string()));
        Ok(Value::Record(event))
    }
}
