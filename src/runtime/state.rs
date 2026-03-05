use crate::runtime::atomic::{AtomicContext, AtomicTransaction};
use crate::runtime::builtins::BuiltinRegistry;
use crate::runtime::env::{Environment, Value};
use crate::runtime::security::{AuditEvent, SecurityPolicy, TrustMode};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[derive(Default)]
pub(crate) struct ModuleLoader {
    pub(crate) cache: HashMap<String, HashMap<String, Value>>,
    pub(crate) loading: HashSet<String>,
}

pub struct Runtime {
    pub env: Environment,
    pub builtins: BuiltinRegistry,
    pub limits: RuntimeLimits,
    /// Directory of the currently executing script (for resolving imports)
    pub script_dir: Option<String>,
    pub(crate) security_policy: SecurityPolicy,
    pub(crate) trust_mode: TrustMode,
    pub(crate) audit_log: Vec<AuditEvent>,
    pub(crate) atomic_active: bool,
    pub(crate) atomic_context: Option<AtomicContext>,
    pub(crate) atomic_txn: Option<AtomicTransaction>,
    pub(crate) callable_sinks: HashSet<String>,
    pub(crate) shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub(crate) module_loader: Arc<RwLock<ModuleLoader>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchDropPolicy {
    DropNewest,
}

#[derive(Debug, Clone)]
pub struct RuntimeLimits {
    pub max_file_size_bytes: usize,
    pub max_rows: usize,
    pub max_pipeline_memory_bytes: usize,
    pub max_event_burst: usize,
    pub watch_queue_capacity: usize,
    pub watch_drop_policy: WatchDropPolicy,
    pub timeout_budget: Duration,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            max_file_size_bytes: read_usize_env("LOOM_MAX_FILE_SIZE_BYTES", 32 * 1024 * 1024),
            max_rows: read_usize_env("LOOM_MAX_ROWS", 100_000),
            max_pipeline_memory_bytes: read_usize_env(
                "LOOM_MAX_PIPELINE_MEMORY_BYTES",
                128 * 1024 * 1024,
            ),
            max_event_burst: read_usize_env("LOOM_MAX_EVENT_BURST", 10_000),
            watch_queue_capacity: read_usize_env("LOOM_WATCH_QUEUE_CAPACITY", 2048),
            watch_drop_policy: WatchDropPolicy::DropNewest,
            timeout_budget: Duration::from_millis(read_u64_env("LOOM_TIMEOUT_BUDGET_MS", 30_000)),
        }
    }
}

fn read_usize_env(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn read_u64_env(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}
