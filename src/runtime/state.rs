use crate::runtime::atomic::{AtomicContext, AtomicTransaction};
use crate::runtime::builtins::BuiltinRegistry;
use crate::runtime::env::{Environment, Value};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

#[derive(Default)]
pub(crate) struct ModuleLoader {
    pub(crate) cache: HashMap<String, HashMap<String, Value>>,
    pub(crate) loading: HashSet<String>,
}

pub struct Runtime {
    pub env: Environment,
    pub builtins: BuiltinRegistry,
    /// Directory of the currently executing script (for resolving imports)
    pub script_dir: Option<String>,
    pub(crate) atomic_active: bool,
    pub(crate) atomic_context: Option<AtomicContext>,
    pub(crate) atomic_txn: Option<AtomicTransaction>,
    pub(crate) callable_sinks: HashSet<String>,
    pub(crate) shutdown_tx: tokio::sync::watch::Sender<bool>,
    pub(crate) module_loader: Rc<RefCell<ModuleLoader>>,
}
