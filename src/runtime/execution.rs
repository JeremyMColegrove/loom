use crate::ast::{Program, Statement};
use crate::runtime::builtins::BuiltinRegistry;
use crate::runtime::env::Value;
use crate::runtime::error::RuntimeResult;
use crate::runtime::{ModuleLoader, Runtime};
use log::debug;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

impl Runtime {
    pub fn new() -> Self {
        let mut env = crate::runtime::env::Environment::new();
        env.set("null", Value::Null);
        let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
        Self {
            env,
            builtins: BuiltinRegistry::new(),
            script_dir: None,
            atomic_active: false,
            atomic_context: None,
            atomic_txn: None,
            callable_sinks: HashSet::new(),
            shutdown_tx,
            module_loader: Rc::new(RefCell::new(ModuleLoader::default())),
        }
    }

    pub fn with_script_dir(mut self, dir: &str) -> Self {
        self.script_dir = Some(dir.to_string());
        self
    }

    pub fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn shutdown_trigger(&self) -> tokio::sync::watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    pub(crate) fn subscribe_shutdown(&self) -> tokio::sync::watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    pub async fn execute(&mut self, program: &Program) -> Result<(), String> {
        let result: RuntimeResult<()> = async {
            for stmt in &program.statements {
                match stmt {
                    Statement::Comment(_) => {}
                    Statement::Pipe(flow) => {
                        self.execute_flow(flow).await?;
                    }
                    Statement::Import(import) => {
                        self.execute_import(import).await?;
                    }
                    Statement::Function(func_def) => {
                        self.env.register_function(func_def.clone());
                        debug!("registered function: {}", func_def.name);
                    }
                }
            }
            Ok(())
        }
        .await;
        result.map_err(|e| e.to_string())
    }
}
