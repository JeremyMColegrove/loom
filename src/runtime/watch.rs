use crate::ast::*;
use crate::runtime::Runtime;
use crate::runtime::WatchDropPolicy;
use crate::runtime::env::Value;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use crate::runtime::security::{AuditOperation, Capability};
use log::warn;
use notify::{
    Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher, event::ModifyKind,
    event::RenameMode,
};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc;
use tokio::time::{Instant as TokioInstant, sleep_until};

impl Runtime {
    pub(crate) fn execute_watch_flow<'a>(
        &'a mut self,
        flow: &'a PipeFlow,
        watch: &'a DirectiveFlow,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Value>> + 'a>> {
        Box::pin(async move {
            let mut evaluated = Vec::new();
            for arg in &watch.arguments {
                evaluated.push(self.eval_expression(arg).await?);
            }
            let watch_opts = self.parse_watch_options(evaluated)?;
            let watch_path_raw = watch_opts.path;
            let watch_path = self.absolutize_watch_path(&watch_path_raw).await?;

            let (tx, mut rx) =
                mpsc::channel::<Result<Event, notify::Error>>(self.limits.watch_queue_capacity);
            let dropped_events = Arc::new(AtomicUsize::new(0));
            let dropped_events_in_cb = dropped_events.clone();
            let drop_policy = self.limits.watch_drop_policy;
            let mut watcher = RecommendedWatcher::new(
                move |res| match tx.try_send(res) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        if matches!(drop_policy, WatchDropPolicy::DropNewest) {
                            dropped_events_in_cb.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
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
            let mut flush_at: Option<TokioInstant> = None;
            let mut dropped_in_burst = 0usize;

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
                        let mut coalesced = Vec::new();
                        for (path, event_type) in Self::flatten_notify_event(event) {
                            if pending.len() >= self.limits.max_event_burst && !pending.contains_key(&path) {
                                dropped_in_burst += 1;
                                continue;
                            }
                            coalesced.push((path, event_type));
                        }
                        Self::coalesce_pending_events(&mut pending, coalesced);
                        if !pending.is_empty() {
                            flush_at = Some(TokioInstant::now() + debounce);
                        }
                    }
                    _ = async {
                        if let Some(at) = flush_at {
                            sleep_until(at).await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    }, if flush_at.is_some() => {
                        let queue_drops = dropped_events.swap(0, Ordering::Relaxed);
                        if queue_drops > 0 {
                            warn!(
                                "@watch dropped {} events due to full queue (policy=drop_newest, capacity={})",
                                queue_drops, self.limits.watch_queue_capacity
                            );
                        }
                        if dropped_in_burst > 0 {
                            warn!(
                                "@watch dropped {} events due to max_event_burst limit ({})",
                                dropped_in_burst, self.limits.max_event_burst
                            );
                            dropped_in_burst = 0;
                        }

                        let events = std::mem::take(&mut pending);
                        flush_at = None;
                        for (path, event_type) in events {
                            let normalized_event_type =
                                Self::normalize_event_type(&path, &event_type);
                            let event = self
                                .make_watch_event(&path, normalized_event_type.as_str())
                                .await?;
                            self.enforce_memory_limit(&event, "watch event")?;
                            match self.run_watch_event(flow, watch, event).await {
                                Ok(_) => {}
                                Err(RuntimeError::FilterRejected) => continue,
                                Err(e) => return Err(e),
                            }
                        }
                    }
                }
            }

            Ok(Value::Null)
        })
    }

    pub(crate) fn absolutize_watch_path<'a>(
        &'a mut self,
        watch_path: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<String>> + 'a>> {
        Box::pin(async move {
            let canonical = self.authorize_watch_path(watch_path)?;
            Ok(canonical.to_string_lossy().to_string())
        })
    }

    pub(crate) fn run_watch_event<'a>(
        &'a mut self,
        flow: &'a PipeFlow,
        watch: &'a DirectiveFlow,
        event: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Value>> + 'a>> {
        Box::pin(async move {
            if let Some(alias) = &watch.alias {
                self.env.set(alias, event.clone());
            }
            let started_at = Instant::now();
            self.run_flow_operations(flow, event, false, started_at)
                .await
        })
    }

    pub(crate) fn make_watch_event<'a>(
        &'a mut self,
        file_path: &'a str,
        event_type: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Value>> + 'a>> {
        Box::pin(async move {
            let metadata = self
                .authorize_existing_path(Capability::Read, AuditOperation::Read, file_path)
                .ok()
                .and_then(|p| std::fs::metadata(p).ok());
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
        })
    }

    fn parse_watch_options(&self, args: Vec<Value>) -> RuntimeResult<WatchOptions> {
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
                        return Err(RuntimeError::message(
                            "@watch debounce must be a non-negative number",
                        ));
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
                                return Err(RuntimeError::message(
                                    "@watch option `debounce_ms` must be a non-negative number",
                                ));
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
            EventKind::Modify(ModifyKind::Name(RenameMode::To)) => event
                .paths
                .into_iter()
                .map(|p| (p.to_string_lossy().to_string(), "created"))
                .collect(),
            EventKind::Modify(ModifyKind::Name(RenameMode::From)) => event
                .paths
                .into_iter()
                .map(|p| (p.to_string_lossy().to_string(), "deleted"))
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
            EventKind::Modify(ModifyKind::Name(RenameMode::Any)) => event
                .paths
                .into_iter()
                .map(|p| (p.to_string_lossy().to_string(), "renamed"))
                .collect(),
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

    fn coalesce_pending_events(
        pending: &mut std::collections::HashMap<String, String>,
        events: Vec<(String, &'static str)>,
    ) {
        for (path, event_type) in events {
            match pending.get(path.as_str()) {
                // Keep `created` through the debounce window so ingest filters that
                // look for event.type == "created" don't get downgraded by follow-up writes.
                Some(existing) if existing == "created" && event_type == "modified" => {}
                _ => {
                    pending.insert(path, event_type.to_string());
                }
            }
        }
    }

    fn normalize_event_type(path: &str, event_type: &str) -> String {
        let exists = Path::new(path).exists();
        match event_type {
            "renamed" => {
                if exists {
                    "created".to_string()
                } else {
                    "deleted".to_string()
                }
            }
            "created" | "modified" if !exists => "deleted".to_string(),
            _ => event_type.to_string(),
        }
    }
}

struct WatchOptions {
    path: String,
    recursive: bool,
    debounce_ms: u64,
}


include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/unit/runtime_watch_tests.rs"));
