use crate::ast::*;
use crate::runtime::Runtime;
use crate::runtime::env::Value;
use notify::{
    Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher, event::ModifyKind,
    event::RenameMode,
};
use std::path::Path;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};

impl Runtime {
    pub(crate) fn execute_watch_flow<'a>(
        &'a mut self,
        flow: &'a PipeFlow,
        watch: &'a DirectiveFlow,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            let mut evaluated = Vec::new();
            for arg in &watch.arguments {
                evaluated.push(self.eval_expression(arg).await?);
            }
            let watch_opts = self.parse_watch_options(evaluated)?;
            let watch_path_raw = watch_opts.path;
            let watch_path = self.absolutize_watch_path(&watch_path_raw)?;

            let (tx, mut rx) = mpsc::unbounded_channel::<Result<Event, notify::Error>>();
            let mut watcher = RecommendedWatcher::new(
                move |res| {
                    let _ = tx.send(res);
                },
                Config::default(),
            )
            .map_err(|e| format!("Failed to initialize file watcher: {}", e))?;

            let watch_mode = if watch_opts.recursive {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };
            watcher
                .watch(Path::new(&watch_path), watch_mode)
                .map_err(|e| format!("Failed to watch '{}': {}", watch_path, e))?;

            let mut shutdown = self.subscribe_shutdown();
            let mut pending = std::collections::HashMap::<String, String>::new();
            let debounce = Duration::from_millis(watch_opts.debounce_ms);
            let mut flush_at: Option<Instant> = None;

            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break;
                        }
                    }
                    maybe_event = rx.recv() => {
                        let Some(event_result) = maybe_event else {
                            break;
                        };
                        let event = event_result.map_err(|e| format!("Watch event stream failed: {}", e))?;
                        for (path, event_type) in Self::flatten_notify_event(event) {
                            pending.insert(path, event_type.to_string());
                        }
                        if !pending.is_empty() {
                            flush_at = Some(Instant::now() + debounce);
                        }
                    }
                    _ = async {
                        if let Some(at) = flush_at {
                            sleep_until(at).await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    }, if flush_at.is_some() => {
                        let events = std::mem::take(&mut pending);
                        flush_at = None;
                        for (path, event_type) in events {
                            let event = self.make_watch_event(&path, &event_type)?;
                            let _ = self.run_watch_event(flow, watch, event).await?;
                        }
                    }
                }
            }

            Ok(Value::Null)
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

    pub(crate) fn run_watch_event<'a>(
        &'a mut self,
        flow: &'a PipeFlow,
        watch: &'a DirectiveFlow,
        event: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            if let Some(alias) = &watch.alias {
                self.env.set(alias, event.clone());
            }
            self.run_flow_operations(flow, event, false).await
        })
    }

    pub(crate) fn make_watch_event(
        &self,
        file_path: &str,
        event_type: &str,
    ) -> Result<Value, String> {
        let metadata = std::fs::metadata(file_path).ok();
        let modified = metadata
            .as_ref()
            .and_then(|m| m.modified().ok())
            .unwrap_or(SystemTime::now());
        let secs = modified
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let approx_year = 1970 + (secs / (365 * 24 * 60 * 60)) as i64;

        let mut created_at = std::collections::HashMap::new();
        created_at.insert("year".to_string(), Value::Number(approx_year as f64));

        let mut file_record = std::collections::HashMap::new();
        let path = Path::new(file_path);
        file_record.insert("path".to_string(), Value::String(file_path.to_string()));
        file_record.insert(
            "name".to_string(),
            Value::String(
                path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string(),
            ),
        );
        file_record.insert(
            "ext".to_string(),
            Value::String(
                path.extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("")
                    .to_string(),
            ),
        );
        file_record.insert("created_at".to_string(), Value::Record(created_at));

        let mut event = std::collections::HashMap::new();
        event.insert("file".to_string(), Value::Record(file_record));
        event.insert("path".to_string(), Value::String(file_path.to_string()));
        event.insert("type".to_string(), Value::String(event_type.to_string()));
        Ok(Value::Record(event))
    }

    fn parse_watch_options(&self, args: Vec<Value>) -> Result<WatchOptions, String> {
        let mut path = ".".to_string();
        let mut recursive = false;
        let mut debounce_ms = 200_u64;

        if let Some(first) = args.first() {
            path = first
                .as_path()
                .ok_or_else(|| "@watch(path) requires the first argument to be a path".to_string())?
                .to_string();
        }

        for arg in args.iter().skip(1) {
            match arg {
                Value::Boolean(flag) => recursive = *flag,
                Value::Number(ms) => {
                    if !ms.is_finite() || *ms < 0.0 {
                        return Err("@watch debounce must be a non-negative number".to_string());
                    }
                    debounce_ms = (*ms as u64).max(10);
                }
                Value::Record(map) => {
                    if let Some(Value::Boolean(flag)) = map.get("recursive") {
                        recursive = *flag;
                    }
                    if let Some(val) = map.get("debounce_ms").or_else(|| map.get("debounce")) {
                        match val {
                            Value::Number(ms) if ms.is_finite() && *ms >= 0.0 => {
                                debounce_ms = (*ms as u64).max(10);
                            }
                            _ => {
                                return Err(
                                    "@watch option `debounce_ms` must be a non-negative number"
                                        .to_string(),
                                );
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(WatchOptions {
            path,
            recursive,
            debounce_ms,
        })
    }

    fn flatten_notify_event(event: Event) -> Vec<(String, &'static str)> {
        match event.kind {
            EventKind::Create(_) => event
                .paths
                .into_iter()
                .map(|p| (p.to_string_lossy().to_string(), "created"))
                .collect(),
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
                if event.paths.len() >= 2 {
                    vec![
                        (event.paths[0].to_string_lossy().to_string(), "deleted"),
                        (event.paths[1].to_string_lossy().to_string(), "created"),
                    ]
                } else {
                    event
                        .paths
                        .into_iter()
                        .map(|p| (p.to_string_lossy().to_string(), "modified"))
                        .collect()
                }
            }
            EventKind::Modify(_) | EventKind::Any => event
                .paths
                .into_iter()
                .map(|p| (p.to_string_lossy().to_string(), "modified"))
                .collect(),
            EventKind::Remove(_) => event
                .paths
                .into_iter()
                .map(|p| (p.to_string_lossy().to_string(), "deleted"))
                .collect(),
            EventKind::Access(_) | EventKind::Other => Vec::new(),
        }
    }
}

struct WatchOptions {
    path: String,
    recursive: bool,
    debounce_ms: u64,
}
