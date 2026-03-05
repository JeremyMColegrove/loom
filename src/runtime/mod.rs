pub mod env;
pub mod builtins;
pub mod fs;

use crate::ast::*;
use crate::runtime::env::Value;
use crate::runtime::builtins::BuiltinRegistry;
use crate::runtime::fs::{AtomicContext, AtomicTransaction};
use crate::parser;
use std::collections::HashSet;

use std::path::Path;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

pub struct Runtime {
    pub env: env::Environment,
    pub builtins: BuiltinRegistry,
    /// Directory of the currently executing script (for resolving imports)
    pub script_dir: Option<String>,
    atomic_active: bool,
    atomic_context: Option<AtomicContext>,
    atomic_txn: Option<AtomicTransaction>,
    callable_sinks: HashSet<String>,
}

impl Runtime {
    pub fn new() -> Self {
        let mut env = env::Environment::new();
        env.set("null", Value::Null);
        Self {
            env,
            builtins: BuiltinRegistry::new(),
            script_dir: None,
            atomic_active: false,
            atomic_context: None,
            atomic_txn: None,
            callable_sinks: HashSet::new(),
        }
    }

    pub fn with_script_dir(mut self, dir: &str) -> Self {
        self.script_dir = Some(dir.to_string());
        self
    }

    pub async fn execute(&mut self, program: &Program) -> Result<(), String> {
        for stmt in &program.statements {
            match stmt {
                Statement::Pipe(flow) => {
                    self.execute_flow(flow).await?;
                }
                Statement::Import(import) => {
                    self.execute_import(import).await?;
                }
                Statement::Function(func_def) => {
                    self.env.register_function(func_def.clone());
                    println!("  🔧 Registered function: {}", func_def.name);
                }
            }
        }
        Ok(())
    }

    fn execute_import<'a>(&'a mut self, import: &'a ImportStmt) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + 'a>> {
        Box::pin(async move {
            let path_str = &import.path;

            if path_str.starts_with("std") {
                return self.register_std_import(import);
            }

            let base_dir = self.script_dir.clone().unwrap_or_default();

            // Try the path as-is first, then with dots replaced by path separators
            let candidates = vec![
                path_str.clone(),
                path_str.replace('.', "/"),
            ];

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

            let import_path = import_path
                .ok_or_else(|| format!("Import module not found: {}", path_str))?;

            let content = std::fs::read_to_string(&import_path)
                .map_err(|e| format!("Failed to read module: {}", e))?;

            let parsed = crate::parser::parse(&content)
                .map_err(|errors| {
                    let msgs: Vec<String> = errors.iter()
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

            println!("  📦 Imported {} as {}", path_str, import.alias.clone().unwrap_or_else(|| path_str.clone()));
            Ok(())
        })
    }

    fn register_std_import(&mut self, import: &ImportStmt) -> Result<(), String> {
        let path_str = &import.path;
        let path = path_str
            .trim_end_matches(".loom")
            .replace('/', ".");
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
                    name: parse_name.clone(),
                    parameters: vec!["input".to_string()],
                    body: FlowOrBranch::Flow(PipeFlow {
                        source: Source::Expression(Expression::Identifier("input".to_string())),
                        operations: vec![(
                            PipeOp::Safe,
                            Destination::Directive(DirectiveFlow {
                                name: "csv.parse".to_string(),
                                arguments: vec![],
                                alias: None,
                            }),
                        )],
                        on_fail: None,
                    }),
                };
                exports.insert("parse".to_string(), Value::Function(parse_func.clone()));

                if let Some(alias) = &import.alias {
                    self.env.set(alias, Value::Record(exports));
                } else {
                    self.env.register_function(parse_func);
                }
                self.callable_sinks.insert(parse_name.clone());

                let label = import.alias.clone().unwrap_or_else(|| path_str.clone());
                println!("  📦 Imported standard module: {}", label);
                Ok(())
            }
            "out" => {
                let sink_name = import.alias.clone().unwrap_or_else(|| "out".to_string());
                self.env.register_function(FunctionDef {
                    name: sink_name.clone(),
                    parameters: vec!["input".to_string()],
                    body: FlowOrBranch::Flow(PipeFlow {
                        source: Source::Expression(Expression::Identifier("input".to_string())),
                        operations: vec![(
                            PipeOp::Safe,
                            Destination::FunctionCall(FunctionCall {
                                name: "print".to_string(),
                                arguments: vec![],
                                alias: None,
                            }),
                        )],
                        on_fail: None,
                    }),
                });
                self.callable_sinks.insert(sink_name.clone());
                let label = import.alias.clone().unwrap_or_else(|| path_str.clone());
                println!("  📦 Imported standard module: {}", label);
                Ok(())
            }
            _ => Err(format!("Unknown standard module: '{}'", path_str)),
        }
    }



    pub fn execute_flow<'a>(&'a mut self, flow: &'a PipeFlow) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
        let atomic_was_active_before_source = self.atomic_active;
        if let Source::Directive(directive) = &flow.source {
            if directive.name == "watch" {
                return self.execute_watch_flow(flow, directive).await;
            }
        }

        // Evaluate the source
        let current_value = match self.eval_source(&flow.source).await {
            Ok(val) => val,
            Err(e) => {
                if let Some(on_fail) = &flow.on_fail {
                    return self.handle_on_fail(on_fail, &e).await;
                }
                return Err(e);
            }
        };
        let atomic_started_here = !atomic_was_active_before_source && self.atomic_active;

        // Bind alias if source is a FunctionCall with `as`
        if let Source::FunctionCall(call) = &flow.source {
            if let Some(alias) = &call.alias {
                self.env.set(alias, current_value.clone());
            }
        }

        self.run_flow_operations(flow, current_value, atomic_started_here).await
        })
    }

    fn execute_watch_flow<'a>(&'a mut self, flow: &'a PipeFlow, watch: &'a DirectiveFlow) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
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

    fn absolutize_watch_path(&self, watch_path: &str) -> Result<String, String> {
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

    fn run_watch_event<'a>(&'a mut self, flow: &'a PipeFlow, watch: &'a DirectiveFlow, event: Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            if let Some(alias) = &watch.alias {
                self.env.set(alias, event.clone());
            }
            self.run_flow_operations(flow, event, false).await
        })
    }

    fn handle_on_fail<'a>(&'a mut self, on_fail: &'a OnFail, error: &'a str) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
        self.env.push_scope();
        
        let error_val = Value::String(error.to_string());
        if let Some(alias) = &on_fail.alias {
            self.env.set(alias, error_val.clone());
        }
        self.env.set("err", error_val);

        let result = match on_fail.handler.as_ref() {
            FlowOrBranch::Flow(flow) => self.execute_flow(flow).await,
            FlowOrBranch::Branch(branch) => self.execute_branch(branch).await,
        };

        self.env.pop_scope();
        result
        })
    }

    fn execute_branch<'a>(&'a mut self, branch: &'a Branch) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
        let mut last_value = Value::Null;
        for flow in &branch.flows {
            last_value = self.execute_flow(flow).await?;
        }
        Ok(last_value)
        })
    }

    fn execute_branch_with_input<'a>(&'a mut self, branch: &'a Branch, input: Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            let mut last_value = input.clone();
            for flow in &branch.flows {
                self.env.push_scope();
                self.env.set("_", input.clone());
                let result = self.run_branch_flow_with_input(flow, input.clone()).await;
                self.env.pop_scope();
                match result {
                    Ok(v) => last_value = v,
                    Err(e) if e == "Filter condition failed" => continue,
                    Err(e) => return Err(e),
                }
            }
            Ok(last_value)
        })
    }

    fn run_branch_flow_with_input<'a>(&'a mut self, flow: &'a PipeFlow, input: Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            if flow.operations.is_empty() {
                if let Source::Expression(Expression::Literal(Literal::Path(path))) = &flow.source {
                    return self.write_or_move_path(&PipeOp::Safe, path, &input);
                }
            }
            let mut current_value = self.eval_branch_source_with_input(&flow.source, input).await?;
            for (op, dest) in &flow.operations {
                current_value = self.eval_destination(op, dest, current_value).await?;
            }
            Ok(current_value)
        })
    }

    fn eval_branch_source_with_input<'a>(&'a mut self, source: &'a Source, input: Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            match source {
                Source::Directive(directive) => self.eval_directive_with_pipe(directive, input).await,
                Source::FunctionCall(call) => self.eval_function_call_with_pipe(call, input).await,
                Source::Expression(expr) => match expr {
                    Expression::Literal(Literal::Path(path)) => Ok(Value::Path(path.clone())),
                    Expression::Identifier(name) if name == "_" => Ok(input),
                    _ => self.eval_expression(expr).await,
                },
            }
        })
    }

    fn eval_source<'a>(&'a mut self, source: &'a Source) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            match source {
                Source::Directive(directive) => self.eval_directive(directive).await,
                Source::FunctionCall(call) => self.eval_function_call(call).await,
                Source::Expression(expr) => {
                    match expr {
                        Expression::Literal(Literal::Path(path)) => Ok(Value::Path(path.clone())),
                        _ => self.eval_expression(expr).await,
                    }
                }
            }
        })
    }

    fn run_flow_operations<'a>(&'a mut self, flow: &'a PipeFlow, mut current_value: Value, atomic_started_here_initial: bool) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            let mut atomic_started_here = atomic_started_here_initial;

            for (op, dest) in &flow.operations {
                let was_active = self.atomic_active;
                let result = self.eval_destination(op, dest, current_value.clone()).await;
                // Detect if @atomic was activated by this operation
                if !atomic_started_here && !was_active && self.atomic_active {
                    atomic_started_here = true;
                }
                match result {
                    Ok(val) => {
                        current_value = val;
                    }
                    Err(e) => {
                        if atomic_started_here {
                            self.rollback_atomic()?;
                        }
                        match op {
                            PipeOp::Force => {
                                println!("  ⚠️  Force pipe ignoring error: {}", e);
                            }
                            PipeOp::Safe | PipeOp::Move => {
                                if let Some(on_fail) = &flow.on_fail {
                                    return self.handle_on_fail(on_fail, &e).await;
                                }
                                return Err(e);
                            }
                        }
                    }
                }
            }

            if atomic_started_here {
                self.commit_atomic()?;
            }
            Ok(current_value)
        })
    }

    fn eval_destination<'a>(&'a mut self, op: &'a PipeOp, dest: &'a Destination, pipe_val: Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
        match dest {
            Destination::Directive(directive) => {
                // Directive receives the pipe value
                self.eval_directive_with_pipe(directive, pipe_val).await
            }
            Destination::FunctionCall(call) => {
                // Function receives pipe value as implicit first arg
                self.eval_function_call_with_pipe(call, pipe_val).await
            }
            Destination::Branch(branch) => {
                self.execute_branch_with_input(branch, pipe_val).await
            }
            Destination::Expression(expr) => {
                match expr {
                    Expression::Literal(Literal::Path(path)) => {
                        self.write_or_move_path(op, path, &pipe_val)
                    }
                    Expression::Identifier(name) => {
                        if self.callable_sinks.contains(name) && self.env.get_function(name).is_some() {
                            return self.call_function(name, vec![pipe_val]).await;
                        }
                        // Store value in variable
                        self.env.set(name, pipe_val.clone());
                        Ok(pipe_val)
                    }
                    Expression::MemberAccess(parts) => {
                        // Allow shorthand callable syntax in pipelines:
                        // `value >> module.func` dispatches to `module.func(value)`.
                        let qualified = parts.join(".");
                        // Check direct function registry first
                        if self.env.get_function(&qualified).is_some() {
                            return self.call_function(&qualified, vec![pipe_val]).await;
                        }
                        // Check if it resolves to a function value inside a bound record
                        if let Some(func_def) = self.resolve_function(&qualified) {
                            let _ = func_def;
                            return self.call_function(&qualified, vec![pipe_val]).await;
                        }
                        self.eval_expression(expr).await
                    }
                    _ => {
                        let val = self.eval_expression(expr).await?;
                        match &val {
                            Value::String(s) => self.write_or_move_path(op, s, &pipe_val),
                            Value::Path(p) => self.write_or_move_path(op, p, &pipe_val),
                            _ => Ok(val),
                        }
                    }
                }
            }
        }
        })
    }

    fn eval_directive<'a>(&'a mut self, directive: &'a DirectiveFlow) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        self.eval_directive_with_pipe(directive, Value::Null)
    }

    fn eval_directive_with_pipe<'a>(&'a mut self, directive: &'a DirectiveFlow, pipe_val: Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
        let mut args = Vec::new();
        for arg in &directive.arguments {
            args.push(self.eval_expression(arg).await?);
        }

        println!("  ⚙️  @{}{}", directive.name,
            if args.is_empty() { String::new() }
            else { format!("({})", args.iter().map(|a| a.as_string()).collect::<Vec<_>>().join(", ")) });

        if directive.name == "atomic" {
            if !self.atomic_active {
                self.begin_atomic()?;
            }
            if let Some(alias) = &directive.alias {
                self.env.set(alias, pipe_val.clone());
            }
            return Ok(pipe_val);
        }

        if directive.name == "filter" {
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

        if directive.name == "map" {
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

        let result = if let Some(handler) = self.builtins.get_directive(&directive.name) {
            handler(args, pipe_val)?
        } else if directive.name.ends_with(".parse") {
            let mut record = std::collections::HashMap::new();
            record.insert("source".to_string(), Value::String(pipe_val.as_string()));
            record.insert("valid".to_string(), Value::Boolean(true));
            record.insert("rows".to_string(), Value::List(vec![]));
            Value::Record(record)
        } else {
            return Err(format!("Unknown directive: @{}", directive.name));
        };

        // Bind the alias if present
        if let Some(alias) = &directive.alias {
            self.env.set(alias, result.clone());
        }

        Ok(result)
        })
    }

    fn eval_function_call<'a>(&'a mut self, call: &'a FunctionCall) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            let mut args = Vec::new();
            for arg in &call.arguments {
                args.push(self.eval_expression(arg).await?);
            }
            self.call_function(&call.name, args).await
        })
    }

    fn eval_function_call_with_pipe<'a>(&'a mut self, call: &'a FunctionCall, pipe_val: Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            let mut args: Vec<Value> = vec![pipe_val];
            for arg in &call.arguments {
                args.push(self.eval_expression(arg).await?);
            }
            let result = self.call_function(&call.name, args).await?;
            if let Some(alias) = &call.alias {
                self.env.set(alias, result.clone());
            }
            Ok(result)
        })
    }

    fn resolve_function(&self, name: &str) -> Option<FunctionDef> {
        if let Some(func_def) = self.env.get_function(name).cloned() {
            return Some(func_def);
        }

        if name.contains('.') {
            let parts: Vec<&str> = name.split('.').collect();
            if let Some(mut val) = self.env.get(parts[0]).cloned() {
                for part in &parts[1..] {
                    val = match val.get_member(part) {
                        Ok(v) => v,
                        Err(_) => return None,
                    };
                }
                if let Value::Function(f) = val {
                    return Some(f);
                }
            }
        }
        None
    }

    fn call_function<'a>(&'a mut self, name: &'a str, args: Vec<Value>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
        if name == "filter" {
            return self.call_filter(args).await;
        }
        if name == "map" {
            return self.call_map(args).await;
        }

        // Check builtins first
        if let Some(handler) = self.builtins.get_builtin_function(name) {
            return handler(args);
        }

        // Check user-defined functions
        if let Some(func_def) = self.resolve_function(name) {
            self.env.push_scope();
            
            // Bind parameters
            for (i, param) in func_def.parameters.iter().enumerate() {
                let val = args.get(i).cloned().unwrap_or(Value::Null);
                self.env.set(param, val);
            }

            println!("  🔧 Calling function: {}({})", name,
                func_def.parameters.join(", "));

            let result = match &func_def.body {
                FlowOrBranch::Flow(flow) => self.execute_flow(flow).await,
                FlowOrBranch::Branch(branch) => self.execute_branch(branch).await,
            };
            self.env.pop_scope();
            result
        } else {
            Err(format!("Unknown function: {}", name))
        }
        })
    }

    fn eval_expression<'a>(&'a mut self, expr: &'a Expression) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
        match expr {
            Expression::Literal(lit) => {
                match lit {
                    Literal::Path(s) => Ok(Value::Path(s.clone())),
                    Literal::String(s) => Ok(Value::String(s.clone())),
                    Literal::Number(n) => Ok(Value::Number(*n)),
                    Literal::Boolean(b) => Ok(Value::Boolean(*b)),
                }
            }
            Expression::Identifier(name) => {
                self.env.get(name)
                    .cloned()
                    .ok_or_else(|| format!("Undefined variable: {}", name))
            }
            Expression::MemberAccess(parts) => {
                if parts.is_empty() {
                    return Err("Invalid member access".to_string());
                }
                let root = &parts[0];
                let mut value = self.env.get(root)
                    .cloned()
                    .ok_or_else(|| format!("Undefined variable: {}", root))?;
                for member in &parts[1..] {
                    value = value.get_member(member)?;
                }
                Ok(value)
            }
            Expression::BinaryOp(left, op, right) => {
                let left_val = self.eval_expression(left).await?;
                match op.as_str() {
                    "&&" => {
                        if !self.is_truthy(&left_val) {
                            Ok(Value::Boolean(false))
                        } else {
                            let right_val = self.eval_expression(right).await?;
                            Ok(Value::Boolean(self.is_truthy(&right_val)))
                        }
                    }
                    "||" => {
                        if self.is_truthy(&left_val) {
                            Ok(Value::Boolean(true))
                        } else {
                            let right_val = self.eval_expression(right).await?;
                            Ok(Value::Boolean(self.is_truthy(&right_val)))
                        }
                    }
                    _ => {
                        let right_val = self.eval_expression(right).await?;
                        self.eval_binary_op(&left_val, op, &right_val)
                    }
                }
            }
            Expression::UnaryOp(op, expr) => {
                let val = self.eval_expression(expr).await?;
                match op.as_str() {
                    "!" => match val {
                        Value::Boolean(b) => Ok(Value::Boolean(!b)),
                        _ => Err(format!("Cannot negate non-boolean: {:?}", val))
                    }
                    _ => Err(format!("Unknown unary operator: {}", op))
                }
            }
            Expression::FunctionCall(call) => {
                let mut args = Vec::new();
                for arg in &call.arguments {
                    args.push(self.eval_expression(arg).await?);
                }
                self.call_function(&call.name, args).await
            }
            Expression::Lambda(lambda) => {
                Ok(Value::Lambda(lambda.clone()))
            }
        }
        })
    }

    fn eval_binary_op(&self, left: &Value, op: &str, right: &Value) -> Result<Value, String> {
        let to_number = |v: &Value| -> Option<f64> {
            match v {
                Value::Number(n) => Some(*n),
                Value::String(s) => s.trim().parse::<f64>().ok(),
                Value::Path(p) => p.trim().parse::<f64>().ok(),
                _ => None,
            }
        };

        match op {
            "+" => {
                match (left, right) {
                    (Value::Number(a), Value::Number(b)) => Ok(Value::Number(a + b)),
                    (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
                    _ => Ok(Value::String(format!("{}{}", left.as_string(), right.as_string())))
                }
            }
            "-" => {
                match (left, right) {
                    (Value::Number(a), Value::Number(b)) => Ok(Value::Number(a - b)),
                    _ => Err("Cannot subtract non-numbers".to_string())
                }
            }
            "*" => {
                match (left, right) {
                    (Value::Number(a), Value::Number(b)) => Ok(Value::Number(a * b)),
                    _ => Err("Cannot multiply non-numbers".to_string())
                }
            }
            "/" => {
                match (left, right) {
                    (Value::Number(a), Value::Number(b)) => {
                        if *b == 0.0 { Err("Division by zero".to_string()) }
                        else { Ok(Value::Number(a / b)) }
                    }
                    _ => Err("Cannot divide non-numbers".to_string())
                }
            }
            "==" => Ok(Value::Boolean(left.as_string() == right.as_string())),
            "!=" => Ok(Value::Boolean(left.as_string() != right.as_string())),
            ">" => {
                match (to_number(left), to_number(right)) {
                    (Some(a), Some(b)) => Ok(Value::Boolean(a > b)),
                    _ => Err("Cannot compare '>' for non-numbers".to_string())
                }
            }
            "<" => {
                match (to_number(left), to_number(right)) {
                    (Some(a), Some(b)) => Ok(Value::Boolean(a < b)),
                    _ => Err("Cannot compare '<' for non-numbers".to_string())
                }
            }
            ">=" => {
                match (to_number(left), to_number(right)) {
                    (Some(a), Some(b)) => Ok(Value::Boolean(a >= b)),
                    _ => Err("Cannot compare '>=' for non-numbers".to_string())
                }
            }
            "<=" => {
                match (to_number(left), to_number(right)) {
                    (Some(a), Some(b)) => Ok(Value::Boolean(a <= b)),
                    _ => Err("Cannot compare '<=' for non-numbers".to_string())
                }
            }
            _ => Err(format!("Unknown operator: {}", op))
        }
    }

    fn is_truthy(&self, value: &Value) -> bool {
        match value {
            Value::Boolean(b) => *b,
            Value::Number(n) => *n != 0.0,
            Value::Null => false,
            _ => !value.as_string().is_empty(),
        }
    }

    fn call_filter<'a>(&'a mut self, args: Vec<Value>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
        if args.len() == 1 {
            let condition = args.first().cloned().unwrap_or(Value::Null);
            if self.is_truthy(&condition) {
                return Ok(Value::Boolean(true));
            } else {
                return Err("Filter condition failed".to_string());
            }
        }

        let list = args.first().cloned().unwrap_or(Value::Null);
        let list = match list {
            Value::Record(map) => map.get("rows").cloned().unwrap_or(Value::Record(map)),
            other => other,
        };
        let condition_or_lambda = args.get(1).cloned().unwrap_or(Value::Null);

        if let Value::Boolean(b) = condition_or_lambda {
            if b {
                return Ok(args.first().cloned().unwrap_or(Value::Null));
            } else {
                return Err("Filter condition failed".to_string());
            }
        }

        let (items, lambda) = match (list, condition_or_lambda) {
            (Value::List(items), Value::Lambda(lambda)) => (items, lambda),
            (Value::List(items), _) => return Ok(Value::List(items)),
            _ => return Err("filter expects a list and a lambda, or a condition".to_string()),
        };

        let mut filtered = Vec::new();
        for item in items {
            self.env.push_scope();
            self.env.set(&lambda.param, item.clone());
            let keep = match self.eval_expression(&lambda.body).await {
                Ok(v) => self.is_truthy(&v),
                Err(e) => {
                    println!("  ⚠️  Filter error on row: {}", e);
                    false
                }
            };
            self.env.pop_scope();
            if keep {
                filtered.push(item);
            }
        }
        Ok(Value::List(filtered))
        })
    }

    fn call_map<'a>(&'a mut self, args: Vec<Value>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
        let list = args.first().cloned().unwrap_or(Value::Null);
        let list = match list {
            Value::Record(map) => map.get("rows").cloned().unwrap_or(Value::Record(map)),
            other => other,
        };
        let lambda = args.get(1).cloned().unwrap_or(Value::Null);
        let (items, lambda) = match (list, lambda) {
            (Value::List(items), Value::Lambda(lambda)) => (items, lambda),
            (Value::List(items), _) => return Ok(Value::List(items)),
            _ => return Err("map expects a list as first argument".to_string()),
        };

        let mut mapped = Vec::new();
        for item in items {
            self.env.push_scope();
            self.env.set(&lambda.param, item);
            let mapped_item = self.eval_expression(&lambda.body).await?;
            self.env.pop_scope();
            mapped.push(mapped_item);
        }
        Ok(Value::List(mapped))
        })
    }

    fn scan_watch_path(&self, watch_path: &str) -> Result<std::collections::HashMap<String, SystemTime>, String> {
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

    fn make_watch_event(&self, file_path: &str, event_type: &str) -> Result<Value, String> {
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

    fn write_or_move_path(&mut self, op: &PipeOp, raw_target: &str, pipe_val: &Value) -> Result<Value, String> {
        if matches!(op, PipeOp::Move) {
            return self.move_file(raw_target, pipe_val, op);
        }

        // If the source is a file path and the target looks like a directory, move the file
        if pipe_val.as_path().is_some() && self.is_directory_target(raw_target) {
            return self.move_file(raw_target, pipe_val, op);
        }

        let payload = match pipe_val {
            Value::Path(src) => std::fs::read_to_string(src)
                .map_err(|e| format!("Failed to read '{}': {}", src, e))?,
            _ => self.serialize_for_path_output(pipe_val),
        };



        self.snapshot_if_atomic(raw_target)?;
        match op {
            PipeOp::Safe => {
                if payload.is_empty() {
                    return Ok(Value::Path(raw_target.to_string()));
                }
                
                println!("  📁 Appending to: {}", raw_target);
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .append(true)
                    .open(raw_target)
                    .map_err(|e| format!("Failed to open '{}': {}", raw_target, e))?;
                use std::io::{Read, Seek, SeekFrom, Write};
                
                let mut needs_newline = false;
                if let Ok(meta) = file.metadata() {
                    if meta.len() > 0 {
                        if file.seek(SeekFrom::End(-1)).is_ok() {
                            let mut buf = [0; 1];
                            if file.read_exact(&mut buf).is_ok() && buf[0] != b'\n' {
                                needs_newline = true;
                            }
                        }
                    }
                }
                
                if needs_newline {
                    let _ = file.write_all(b"\n");
                }
                
                let mut bytes = payload.into_bytes();
                if !bytes.ends_with(b"\n") {
                    bytes.push(b'\n');
                }
                
                file.write_all(&bytes)
                    .map_err(|e| format!("Failed to append '{}': {}", raw_target, e))?;
            }
            PipeOp::Force => {
                println!("  📁 Overwriting: {}", raw_target);
                std::fs::write(raw_target, payload)
                    .map_err(|e| format!("Failed to write '{}': {}", raw_target, e))?;
            }
            PipeOp::Move => unreachable!(),
        }

        Ok(Value::Path(raw_target.to_string()))
    }



    fn serialize_for_path_output(&self, value: &Value) -> String {
        self.serialize_csv_if_possible(value)
            .unwrap_or_else(|| value.as_string())
    }

    fn serialize_csv_if_possible(&self, value: &Value) -> Option<String> {
        match value {
            Value::Record(map) => {
                if let Some(Value::List(rows)) = map.get("rows") {
                    let preferred_headers = map.get("headers")
                        .and_then(|h| match h {
                            Value::List(items) => Some(items.iter()
                                .map(|v| v.as_string())
                                .collect::<Vec<_>>()),
                            _ => None,
                        });
                    return self.serialize_records_as_csv(rows, preferred_headers.as_deref());
                }
                None
            }
            Value::List(rows) => self.serialize_records_as_csv(rows, None),
            _ => None,
        }
    }

    fn serialize_records_as_csv(&self, rows: &[Value], preferred_headers: Option<&[String]>) -> Option<String> {
        if rows.is_empty() {
            return Some(String::new());
        }
        if !rows.iter().all(|r| matches!(r, Value::Record(_))) {
            return None;
        }

        let mut headers: Vec<String> = preferred_headers
            .map(|h| h.to_vec())
            .unwrap_or_default();
        let mut seen = std::collections::HashSet::new();
        for h in &headers {
            seen.insert(h.to_ascii_lowercase());
        }
        for row in rows {
            let Value::Record(map) = row else { continue };
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort_by_key(|k| k.to_ascii_lowercase());
            for key in keys {
                let folded = key.to_ascii_lowercase();
                if !seen.contains(&folded) {
                    seen.insert(folded);
                    headers.push(key);
                }
            }
        }

        if headers.is_empty() {
            return Some(String::new());
        }

        let mut out = String::new();
        out.push_str(&headers.iter().map(|h| csv_escape(h)).collect::<Vec<_>>().join(","));
        out.push('\n');
        for row in rows {
            let Value::Record(map) = row else { continue };
            let line = headers.iter()
                .map(|h| {
                    map.iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case(h))
                        .map(|(_, v)| csv_escape(&v.as_string()))
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(",");
            out.push_str(&line);
            out.push('\n');
        }

        Some(out)
    }

    fn is_directory_target(&self, target: &str) -> bool {
        target.ends_with('/') || Path::new(target).is_dir()
    }

    fn move_file(&mut self, raw_target: &str, pipe_val: &Value, op: &PipeOp) -> Result<Value, String> {
        let src_path = match pipe_val.as_path() {
            Some(p) => p.to_string(),
            None => return Err("Move targets require a file path source".to_string()),
        };

        let src = Path::new(&src_path);
        let file_name = src.file_name()
            .ok_or_else(|| format!("Source path has no file name: '{}'", src_path))?;

        let mut target_path = std::path::PathBuf::from(raw_target);
        if !target_path.is_absolute() {
            if let Some(dir) = &self.script_dir {
                target_path = std::path::PathBuf::from(dir).join(target_path);
            }
        }

        if self.is_directory_target(raw_target) {
            std::fs::create_dir_all(&target_path)
                .map_err(|e| format!("Failed to create directory '{}': {}", target_path.display(), e))?;
            target_path = target_path.join(file_name);
        } else {
            if let Some(parent) = target_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("Failed to create directory '{}': {}", parent.display(), e))?;
            }
        }

        let dest = target_path.to_string_lossy().to_string();

        self.snapshot_if_atomic(&src_path)?;
        self.snapshot_if_atomic(&dest)?;

        if matches!(op, PipeOp::Force) && target_path.exists() {
            std::fs::remove_file(&target_path)
                .map_err(|e| format!("Failed to replace '{}': {}", dest, e))?;
        }

        std::fs::rename(src, &target_path)
            .map_err(|e| format!("Failed to move '{}' to '{}': {}", src_path, dest, e))?;

        Ok(Value::Path(dest))
    }

    fn begin_atomic(&mut self) -> Result<(), String> {
        let base = if let Some(dir) = &self.script_dir {
            Path::new(dir).to_path_buf()
        } else {
            std::env::current_dir().map_err(|e| format!("Failed to resolve current directory: {}", e))?
        };
        if self.atomic_context.is_none() {
            self.atomic_context = Some(
                AtomicContext::new(base).map_err(|e| format!("Failed to initialize atomic journal: {}", e))?
            );
        }
        let txn = self.atomic_context
            .as_ref()
            .ok_or_else(|| "Atomic context unavailable".to_string())?
            .begin()
            .map_err(|e| format!("Failed to begin atomic transaction: {}", e))?;
        self.atomic_txn = Some(txn);
        self.atomic_active = true;
        Ok(())
    }

    fn snapshot_if_atomic(&mut self, path: &str) -> Result<(), String> {
        if !self.atomic_active {
            return Ok(());
        }
        if let Some(txn) = self.atomic_txn.as_mut() {
            txn.snapshot_path(path)
                .map_err(|e| format!("Failed to snapshot '{}' for atomic rollback: {}", path, e))?;
        }
        Ok(())
    }

    fn commit_atomic(&mut self) -> Result<(), String> {
        if let Some(txn) = self.atomic_txn.take() {
            txn.commit()
                .map_err(|e| format!("Failed to commit atomic transaction: {}", e))?;
        }
        self.atomic_active = false;
        Ok(())
    }

    fn rollback_atomic(&mut self) -> Result<(), String> {
        if let Some(txn) = self.atomic_txn.take() {
            txn.rollback()
                .map_err(|e| format!("Failed to roll back atomic transaction: {}", e))?;
        }
        self.atomic_active = false;
        Ok(())
    }
}

fn csv_escape(value: &str) -> String {
    let needs_quotes = value.contains(',')
        || value.contains('"')
        || value.contains('\n')
        || value.contains('\r');
    if !needs_quotes {
        return value.to_string();
    }
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn path_source_reads_and_destination_appends() {
        let dir = tempdir().expect("tempdir should be created");
        let input_path = dir.path().join("hello-world.txt");
        let output_path = dir.path().join("another.txt");

        std::fs::write(&input_path, "hello").expect("should write input");

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::Path(
                input_path.to_string_lossy().to_string(),
            ))),
            operations: vec![(
                PipeOp::Safe,
                Destination::Expression(Expression::Literal(Literal::Path(
                    output_path.to_string_lossy().to_string(),
                ))),
            )],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        runtime.execute_flow(&flow).await.expect("flow should execute");
        runtime.execute_flow(&flow).await.expect("flow should execute twice");

        let output = std::fs::read_to_string(&output_path).expect("should read output");
        assert_eq!(output, "hello\nhello\n");
    }

    #[tokio::test]
    async fn path_destination_overwrites_with_force_pipe() {
        let dir = tempdir().expect("tempdir should be created");
        let input_path = dir.path().join("hello-world.txt");
        let output_path = dir.path().join("another.txt");

        std::fs::write(&input_path, "hello").expect("should write input");
        std::fs::write(&output_path, "existing").expect("should seed output");

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::Path(
                input_path.to_string_lossy().to_string(),
            ))),
            operations: vec![(
                PipeOp::Force,
                Destination::Expression(Expression::Literal(Literal::Path(
                    output_path.to_string_lossy().to_string(),
                ))),
            )],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        runtime.execute_flow(&flow).await.expect("flow should execute");

        let output = std::fs::read_to_string(&output_path).expect("should read output");
        assert_eq!(output, "hello");
    }

    #[tokio::test]
    async fn safe_pipe_moves_file_when_destination_is_directory() {
        let dir = tempdir().expect("tempdir should be created");
        let src = dir.path().join("input.txt");
        let target_dir = dir.path().join("archive");
        std::fs::write(&src, "hello").expect("should write input");

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::Path(
                src.to_string_lossy().to_string(),
            ))),
            operations: vec![(
                PipeOp::Safe,
                Destination::Expression(Expression::Literal(Literal::Path(
                    format!("{}/", target_dir.to_string_lossy()),
                ))),
            )],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        runtime.execute_flow(&flow).await.expect("flow should execute");

        let moved = target_dir.join("input.txt");
        assert!(!src.exists(), "source should be moved");
        assert!(moved.exists(), "destination file should exist");
        let output = std::fs::read_to_string(&moved).expect("should read moved file");
        assert_eq!(output, "hello");
    }

    #[tokio::test]
    async fn atomic_rolls_back_file_write_on_failure() {
        let dir = tempdir().expect("tempdir should be created");
        let output_path = dir.path().join("atomic.txt");

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::String("hello".to_string()))),
            operations: vec![
                (
                    PipeOp::Safe,
                    Destination::Directive(DirectiveFlow {
                        name: "atomic".to_string(),
                        arguments: vec![],
                        alias: None,
                    }),
                ),
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        output_path.to_string_lossy().to_string(),
                    ))),
                ),
                (
                    PipeOp::Safe,
                    Destination::Directive(DirectiveFlow {
                        name: "missing.directive".to_string(),
                        arguments: vec![],
                        alias: None,
                    }),
                ),
            ],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        let result = runtime.execute_flow(&flow).await;
        assert!(result.is_err(), "flow should fail");
        assert!(!output_path.exists(), "atomic rollback should remove output");
    }

    #[tokio::test]
    async fn user_blueprint_receives_piped_first_argument() {
        let dir = tempdir().expect("tempdir should be created");
        let output_path = dir.path().join("fn.txt");

        let mut runtime = Runtime::new();
        runtime.env.register_function(FunctionDef {
            name: "identity".to_string(),
            parameters: vec!["input".to_string()],
            body: FlowOrBranch::Flow(PipeFlow {
                source: Source::Expression(Expression::Identifier("input".to_string())),
                operations: vec![],
                on_fail: None,
            }),
        });

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::String("hello".to_string()))),
            operations: vec![
                (
                    PipeOp::Safe,
                    Destination::FunctionCall(FunctionCall {
                        name: "identity".to_string(),
                        arguments: vec![],
                        alias: None,
                    }),
                ),
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        output_path.to_string_lossy().to_string(),
                    ))),
                ),
            ],
            on_fail: None,
        };

        runtime.execute_flow(&flow).await.expect("flow should execute");
        let output = std::fs::read_to_string(output_path).expect("should read output");
        assert_eq!(output, "hello\n");
    }

    #[tokio::test]
    async fn import_alias_supports_qualified_function_calls() {
        let dir = tempdir().expect("tempdir should be created");
        let module_path = dir.path().join("tools.loom");
        std::fs::write(&module_path, "identity(v) => v").expect("should write module");
        let script_path = dir.path().join("main.loom");
        let output_path = dir.path().join("out.txt");

        let script = format!(
            "@import \"tools.loom\" as t\ns\"hello\" >> t.identity() >> \"{}\"",
            output_path.to_string_lossy()
        );
        std::fs::write(&script_path, &script).expect("should write main script");

        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_string_lossy().as_ref());
        runtime.execute(&program).await.expect("script should execute");

        let output = std::fs::read_to_string(output_path).expect("should read output");
        assert_eq!(output, "hello\n");
    }

    #[tokio::test]
    async fn import_resolves_dot_module_paths() {
        let dir = tempdir().expect("tempdir should be created");
        let module_dir = dir.path().join("pkg");
        std::fs::create_dir_all(&module_dir).expect("should create module dir");
        let module_path = module_dir.join("utils.loom");
        std::fs::write(&module_path, "id(v) => v").expect("should write module");
        let output_path = dir.path().join("dot-out.txt");

        let script = format!(
            "@import \"pkg.utils\" as p\ns\"ok\" >> p.id() >> \"{}\"",
            output_path.to_string_lossy()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_string_lossy().as_ref());
        runtime.execute(&program).await.expect("script should execute");

        let output = std::fs::read_to_string(output_path).expect("should read output");
        assert_eq!(output, "ok\n");
    }

    #[tokio::test]
    async fn watch_alias_is_bound_for_event_operations() {
        let dir = tempdir().expect("tempdir should be created");
        let watched = dir.path().join("master.txt");
        std::fs::write(&watched, "hello").expect("should create watched file");

        let watch = DirectiveFlow {
            name: "watch".to_string(),
            arguments: vec![Expression::Literal(Literal::Path(
                watched.to_string_lossy().to_string(),
            ))],
            alias: Some("event".to_string()),
        };

        let flow = PipeFlow {
            source: Source::Directive(watch.clone()),
            operations: vec![
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::MemberAccess(vec![
                        "event".to_string(),
                        "type".to_string(),
                    ])),
                ),
            ],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        let event = runtime
            .make_watch_event(watched.to_string_lossy().as_ref(), "modified")
            .expect("event should be created");

        let result = runtime
            .run_watch_event(&flow, &watch, event)
            .await
            .expect("watch event should execute");
        assert_eq!(result.as_string(), "modified");
    }

    #[tokio::test]
    async fn nested_member_access_path_writes_literal_path_without_read() {
        let dir = tempdir().expect("tempdir should be created");
        let watched = dir.path().join("master.txt");
        let output = dir.path().join("hello_world.txt");
        std::fs::write(&watched, "hello").expect("should create watched file");

        let mut file = std::collections::HashMap::new();
        file.insert(
            "path".to_string(),
            Value::String(watched.to_string_lossy().to_string()),
        );
        let mut event = std::collections::HashMap::new();
        event.insert("file".to_string(), Value::Record(file));

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::String("seed".to_string()))),
            operations: vec![
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::MemberAccess(vec![
                        "event".to_string(),
                        "file".to_string(),
                        "path".to_string(),
                    ])),
                ),
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        output.to_string_lossy().to_string(),
                    ))),
                ),
            ],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        runtime.env.set("event", Value::Record(event));
        runtime.execute_flow(&flow).await.expect("flow should execute");

        let written = std::fs::read_to_string(&output).expect("should read output");
        assert_eq!(written, format!("{}\n", watched.to_string_lossy()));
    }

    #[tokio::test]
    async fn read_directive_reads_nested_member_access_path() {
        let dir = tempdir().expect("tempdir should be created");
        let watched = dir.path().join("master.txt");
        let output = dir.path().join("hello_world.txt");
        std::fs::write(&watched, "hello").expect("should create watched file");

        let mut file = std::collections::HashMap::new();
        file.insert(
            "path".to_string(),
            Value::String(watched.to_string_lossy().to_string()),
        );
        let mut event = std::collections::HashMap::new();
        event.insert("file".to_string(), Value::Record(file));

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::String("seed".to_string()))),
            operations: vec![
                (
                    PipeOp::Safe,
                    Destination::Directive(DirectiveFlow {
                        name: "read".to_string(),
                        arguments: vec![Expression::MemberAccess(vec![
                            "event".to_string(),
                            "file".to_string(),
                            "path".to_string(),
                        ])],
                        alias: None,
                    }),
                ),
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        output.to_string_lossy().to_string(),
                    ))),
                ),
            ],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        runtime.env.set("event", Value::Record(event));
        runtime.execute_flow(&flow).await.expect("flow should execute");

        let written = std::fs::read_to_string(&output).expect("should read output");
        assert_eq!(written, "hello\n");
    }

    #[tokio::test]
    async fn writing_to_two_path_literals_implicitly_reads_first_file() {
        let dir = tempdir().expect("tempdir should be created");
        let dest = dir.path().join("dest.txt");
        let copy = dir.path().join("copy.txt");

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::String(
                "hello, world!".to_string(),
            ))),
            operations: vec![
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        dest.to_string_lossy().to_string(),
                    ))),
                ),
                (
                    PipeOp::Safe,
                    Destination::Expression(Expression::Literal(Literal::Path(
                        copy.to_string_lossy().to_string(),
                    ))),
                ),
            ],
            on_fail: None,
        };

        let mut runtime = Runtime::new();
        runtime.execute_flow(&flow).await.expect("flow should execute");

        let dest_contents = std::fs::read_to_string(&dest).expect("should read dest");
        let copy_contents = std::fs::read_to_string(&copy).expect("should read copy");
        assert_eq!(dest_contents, "hello, world!\n");
        assert_eq!(copy_contents, "hello, world!\n");
    }

    #[tokio::test]
    async fn std_csv_import_registers_parse_function() {
        let script = "@import \"std.csv\" as csv\ns\"name,age
Ada,30\" >> csv.parse() >> parsed";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");

        let parsed = runtime.env.get("parsed").cloned().expect("parsed should be set");
        let Value::Record(root) = parsed else {
            panic!("parsed should be a record");
        };
        let Some(Value::List(rows)) = root.get("rows") else {
            panic!("rows should be a list");
        };
        assert_eq!(rows.len(), 1);
        let Value::Record(row) = rows[0].clone() else {
            panic!("row should be a record");
        };
        let Some(Value::String(name)) = row.get("name") else {
            panic!("name should be present");
        };
        let Some(Value::String(age)) = row.get("age") else {
            panic!("age should be present");
        };
        assert_eq!(name, "Ada");
        assert_eq!(age, "30");
    }

    #[tokio::test]
    async fn std_csv_member_style_call_uses_import_alias() {
        let script = "@import \"std.csv\" as csv\ns\"name,age
Ada,30\" >> csv.parse >> parsed";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");

        let parsed = runtime.env.get("parsed").cloned().expect("parsed should be set");
        let Value::Record(root) = parsed else {
            panic!("parsed should be a record");
        };
        let Some(Value::List(rows)) = root.get("rows") else {
            panic!("rows should be a list");
        };
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn std_csv_parse_reads_file_when_given_path_literal() {
        let dir = tempdir().expect("tempdir should be created");
        let csv_path = dir.path().join("customers.csv");
        std::fs::write(&csv_path, "name,age\nAda,30\nBob,41\n").expect("should write csv");

        let script = format!(
            "@import \"std.csv\" as csv\n\"{}\" >> csv.parse() >> parsed",
            csv_path.to_string_lossy()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");

        let parsed = runtime.env.get("parsed").cloned().expect("parsed should be set");
        let Value::Record(root) = parsed else {
            panic!("parsed should be a record");
        };
        let Some(Value::List(rows)) = root.get("rows") else {
            panic!("rows should be a list");
        };
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn std_csv_parse_reports_missing_file_for_path_literal() {
        let script = "@import \"std.csv\" as csv\n\"missing.csv\" >> csv.parse() >> parsed";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        let err = runtime.execute(&program).await.expect_err("script should fail");
        assert!(err.contains("Failed to read 'missing.csv'"));
    }

    #[tokio::test]
    async fn filter_directive_style_works_with_lambda() {
        let script = "s\"name,Index
Ada,91
Bob,10\" >> csv.parse >> @filter(row >> row.Index == s\"91\") >> filtered";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.env.register_function(FunctionDef {
            name: "csv.parse".to_string(),
            parameters: vec!["input".to_string()],
            body: FlowOrBranch::Flow(PipeFlow {
                source: Source::Expression(Expression::Identifier("input".to_string())),
                operations: vec![(
                    PipeOp::Safe,
                    Destination::Directive(DirectiveFlow {
                        name: "csv.parse".to_string(),
                        arguments: vec![],
                        alias: None,
                    }),
                )],
                on_fail: None,
            }),
        });

        runtime.execute(&program).await.expect("script should execute");
        let filtered = runtime.env.get("filtered").cloned().expect("filtered should be set");
        let Value::List(rows) = filtered else {
            panic!("filtered should be a list");
        };
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn filter_function_style_accepts_csv_parse_record() {
        let script = "s\"name,Index
Ada,91
Bob,10\" >> csv.parse() >> filter(row >> row.Index == s\"91\") >> filtered";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.env.register_function(FunctionDef {
            name: "csv.parse".to_string(),
            parameters: vec!["input".to_string()],
            body: FlowOrBranch::Flow(PipeFlow {
                source: Source::Expression(Expression::Identifier("input".to_string())),
                operations: vec![(
                    PipeOp::Safe,
                    Destination::Directive(DirectiveFlow {
                        name: "csv.parse".to_string(),
                        arguments: vec![],
                        alias: None,
                    }),
                )],
                on_fail: None,
            }),
        });

        runtime.execute(&program).await.expect("script should execute");
        let filtered = runtime.env.get("filtered").cloned().expect("filtered should be set");
        let Value::List(rows) = filtered else {
            panic!("filtered should be a list");
        };
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn filter_with_lowercase_member_and_numeric_comparison_works() {
        let script = "@import \"std.csv\" as csv\ns\"Index,name
89,A
91,B
100,C\" >> csv.parse >> filter(row >> row.index > 90) >> filtered";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");

        let filtered = runtime.env.get("filtered").cloned().expect("filtered should be set");
        let Value::List(rows) = filtered else {
            panic!("filtered should be a list");
        };
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn filter_supports_logical_and_short_circuit() {
        let script = "@import \"std.csv\" as csv\ns\"Index,name
89,A
91,B\" >> csv.parse >> filter(row >> false && missing_value) >> filtered";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");

        let filtered = runtime.env.get("filtered").cloned().expect("filtered should be set");
        let Value::List(rows) = filtered else {
            panic!("filtered should be a list");
        };
        assert_eq!(rows.len(), 0);
    }

    #[tokio::test]
    async fn filter_as_binding_iterates_per_row() {
        // filter(...) as row >> [row >>> output] should iterate per-item
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("out.csv");
        let script = format!(
            "@import \"std.csv\" as csv\ns\"Index,name\n89,A\n91,B\n100,C\" >> csv.parse >> filter(row >> row.index > 90) as row >> [\n    row >>> \"{}\"\n]",
            output.display()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");
        let content = std::fs::read_to_string(&output).expect("output file should exist");
        // Each row should be written individually (force-appended)
        assert!(content.contains("91"), "Expected row with index 91");
        assert!(content.contains("100"), "Expected row with index 100");
        assert!(!content.contains("89"), "Should not contain filtered-out row 89");
    }

    #[tokio::test]
    async fn std_out_import_alias_can_be_used_as_pipe_sink_identifier() {
        let script = "@import \"std.out\" as stdout\ns\"hello\" >> stdout";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");
    }

    #[tokio::test]
    async fn map_function_works_in_pipe_chain() {
        let script = "s\"Index,name\n1,A\n2,B\" >> csv.parse() >> map(row >> row.name) >> names";
        let program = crate::parser::parse(script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.env.register_function(FunctionDef {
            name: "csv.parse".to_string(),
            parameters: vec!["input".to_string()],
            body: FlowOrBranch::Flow(PipeFlow {
                source: Source::Expression(Expression::Identifier("input".to_string())),
                operations: vec![(
                    PipeOp::Safe,
                    Destination::Directive(DirectiveFlow {
                        name: "csv.parse".to_string(),
                        arguments: vec![],
                        alias: None,
                    }),
                )],
                on_fail: None,
            }),
        });

        runtime.execute(&program).await.expect("script should execute");
        let names = runtime.env.get("names").cloned().expect("names should be set");
        let Value::List(items) = names else {
            panic!("names should be a list");
        };
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].as_string(), "A");
        assert_eq!(items[1].as_string(), "B");
    }

    #[tokio::test]
    async fn writing_filtered_csv_rows_outputs_csv_format() {
        let dir = tempdir().expect("tempdir should be created");
        let out = dir.path().join("high.csv");
        let script = format!(
            "@import \"std.csv\" as csv\ns\"Index,name\n89,A\n91,B\n100,C\" >> csv.parse >> filter(row >> row.index > 90) >>> \"{}\"",
            out.to_string_lossy()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");

        let text = std::fs::read_to_string(out).expect("should read output");
        let lines = text.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("Index"));
        assert!(lines[0].contains("name"));
        assert!(lines[1].contains("91"));
        assert!(lines[2].contains("100"));
    }

    #[tokio::test]
    async fn csv_parse_handles_quoted_commas_without_extra_columns() {
        let dir = tempdir().expect("tempdir should be created");
        let input = dir.path().join("quoted-input.csv");
        let out = dir.path().join("quoted.csv");
        std::fs::write(
            &input,
            "Index,Company,City\n1,\"Dominguez, Mcmillan and Donovan\",Bensonview\n2,\"Martin, Lang and Andrade\",West Priscilla\n",
        )
        .expect("should write input");
        let script = format!(
            "@import \"std.csv\" as csv\n\"{}\" >> csv.parse >> filter(row >> row.index > 0) >>> \"{}\"",
            input.to_string_lossy(),
            out.to_string_lossy()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");

        let output = std::fs::read_to_string(&out).expect("should read output");
        assert!(!output.lines().next().unwrap_or("").contains("col13"));

        let mut reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(output.as_bytes());
        let headers = reader.headers().expect("headers should parse").clone();
        assert_eq!(headers.len(), 3);

        let rows = reader.records()
            .map(|r| r.expect("row should parse"))
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert_eq!(&rows[0][1], "Dominguez, Mcmillan and Donovan");
        assert_eq!(&rows[1][1], "Martin, Lang and Andrade");
    }

    #[tokio::test]
    async fn csv_parse_handles_spaces_before_quoted_fields() {
        let dir = tempdir().expect("tempdir should be created");
        let input = dir.path().join("spaced-input.csv");
        std::fs::write(
            &input,
            "Index,Company,City\n1, \"Dominguez, Mcmillan and Donovan\", Bensonview\n",
        )
        .expect("should write input");

        let script = format!(
            "@import \"std.csv\" as csv\n\"{}\" >> csv.parse >> parsed",
            input.to_string_lossy()
        );
        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");

        let parsed = runtime.env.get("parsed").cloned().expect("parsed should be set");
        let Value::Record(root) = parsed else {
            panic!("parsed should be record");
        };
        let Some(Value::List(rows)) = root.get("rows") else {
            panic!("rows should be list");
        };
        let Value::Record(row) = rows[0].clone() else {
            panic!("row should be record");
        };
        let Some(Value::String(company)) = row.get("Company") else {
            panic!("company should be present");
        };
        assert_eq!(company, "Dominguez, Mcmillan and Donovan");
        assert!(!row.contains_key("col4"));
    }

    #[tokio::test]
    async fn branch_flows_consume_same_piped_input_in_order() {
        let dir = tempdir().expect("tempdir should be created");
        let input = dir.path().join("customers.csv");
        let high = dir.path().join("high.csv");
        let audit = dir.path().join("audit_trail.csv");
        std::fs::write(
            &input,
            "Index,Company\n90,Alpha\n92,Beta\n100,Gamma\n",
        )
        .expect("should write input");

        let script = format!(
            "@import \"std.csv\" as csv\n\"{}\" >> csv.parse >> data\ndata >> [ filter(row >> row.Index > 91) >> \"{}\", \"{}\" ]",
            input.to_string_lossy(),
            high.to_string_lossy(),
            audit.to_string_lossy()
        );

        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");

        let high_text = std::fs::read_to_string(&high).expect("high should be written");
        assert!(high_text.contains("92"));
        assert!(high_text.contains("100"));
        assert!(!high_text.contains("90"));

        let audit_text = std::fs::read_to_string(&audit).expect("audit should be written");
        assert!(audit_text.contains("90"));
        assert!(audit_text.contains("92"));
        assert!(audit_text.contains("100"));
    }

    #[tokio::test]
    async fn branch_path_source_keeps_file_contents_without_appending_input() {
        let dir = tempdir().expect("tempdir should be created");
        let backup = dir.path().join("backup.txt");
        let second = dir.path().join("second_backup.txt");

        let script = format!(
            "@atomic >> [ s\"Hello, world!\" >>> \"{}\", \"{}\" >>> \"{}\" ]",
            backup.to_string_lossy(),
            backup.to_string_lossy(),
            second.to_string_lossy()
        );

        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute");

        let backup_text = std::fs::read_to_string(&backup).expect("backup should exist");
        let second_text = std::fs::read_to_string(&second).expect("second should exist");
        assert_eq!(backup_text, "Hello, world!");
        assert_eq!(second_text, "Hello, world!");
    }

    #[tokio::test]
    async fn atomic_branch_failure_rolls_back_and_runs_on_fail_with_pipeop_syntax() {
        let dir = tempdir().expect("tempdir should be created");
        let backup = dir.path().join("backup.txt");
        let second = dir.path().join("second_backup.txt");
        let status = dir.path().join("status.log");
        let failure = dir.path().join("failure.log");

        let script = format!(
            "@atomic >> [ s\"It's working!\" >>> \"{}\", \"{}\" >>> \"{}\", s\"done!\" >>> \"{}\" ] on_fail >> s\"A failure occured!\" >>> \"{}\"",
            backup.to_string_lossy(),
            dir.path().join("backup.txt22").to_string_lossy(),
            second.to_string_lossy(),
            status.to_string_lossy(),
            failure.to_string_lossy()
        );

        let program = crate::parser::parse(&script).expect("script should parse");
        let mut runtime = Runtime::new();
        runtime.execute(&program).await.expect("script should execute via on_fail");

        assert!(!backup.exists(), "atomic rollback should remove first write");
        assert!(!second.exists(), "later steps should not be executed");
        assert!(!status.exists(), "later steps should not be executed");
        let failure_text = std::fs::read_to_string(&failure).expect("on_fail output should exist");
        assert_eq!(failure_text, "A failure occured!");
    }

    #[test]
    fn watch_path_is_resolved_to_absolute_path() {
        let dir = tempdir().expect("tempdir should be created");
        let original_cwd = std::env::current_dir().expect("should read current dir");
        std::env::set_current_dir(dir.path()).expect("should switch cwd");
        std::fs::write("master.txt", "hello").expect("should create file");

        let runtime = Runtime::new();
        let resolved = runtime
            .absolutize_watch_path("./master.txt")
            .expect("path should resolve");

        std::env::set_current_dir(original_cwd).expect("should restore cwd");
        assert!(std::path::Path::new(&resolved).is_absolute());
    }

    // ── @log directive ──────────────────────────────────────────────────

    #[tokio::test]
    async fn log_directive_passes_through_pipe_value() {
        let dir = tempdir().expect("tempdir");
        let out = dir.path().join("out.txt");

        let program = Program {
            statements: vec![Statement::Pipe(PipeFlow {
                source: Source::Expression(Expression::Literal(Literal::String("hello from log".to_string()))),
                operations: vec![
                    (PipeOp::Safe, Destination::Directive(DirectiveFlow {
                        name: "log".to_string(),
                        arguments: vec![],
                        alias: None,
                    })),
                    (PipeOp::Safe, Destination::Expression(Expression::Literal(Literal::Path(
                        out.to_string_lossy().to_string(),
                    )))),
                ],
                on_fail: None,
            })],
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        runtime.execute(&program).await.expect("should execute");

        let content = std::fs::read_to_string(&out).expect("should read output");
        assert!(content.contains("hello from log"));
    }

    // ── @write directive ────────────────────────────────────────────────

    #[tokio::test]
    async fn write_directive_writes_content_to_file() {
        let dir = tempdir().expect("tempdir");
        let out = dir.path().join("written.txt");

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::String("written data".to_string()))),
            operations: vec![(
                PipeOp::Safe,
                Destination::Directive(DirectiveFlow {
                    name: "write".to_string(),
                    arguments: vec![Expression::Literal(Literal::Path(
                        out.to_string_lossy().to_string(),
                    ))],
                    alias: None,
                }),
            )],
            on_fail: None,
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        runtime.execute_flow(&flow).await.expect("should execute");

        let content = std::fs::read_to_string(&out).expect("should read output");
        assert_eq!(content, "written data");
    }

    // ── @lines directive ────────────────────────────────────────────────

    #[tokio::test]
    async fn lines_directive_reads_file_into_list() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("data.txt");
        std::fs::write(&input, "alpha\nbeta\ngamma").expect("write input");

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::Path(
                input.to_string_lossy().to_string(),
            ))),
            operations: vec![(
                PipeOp::Safe,
                Destination::Directive(DirectiveFlow {
                    name: "lines".to_string(),
                    arguments: vec![],
                    alias: Some("data".to_string()),
                }),
            )],
            on_fail: None,
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        let result = runtime.execute_flow(&flow).await.expect("should execute");

        match result {
            Value::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0].as_string(), "alpha");
                assert_eq!(items[1].as_string(), "beta");
                assert_eq!(items[2].as_string(), "gamma");
            }
            _ => panic!("expected list, got {:?}", result),
        }
    }

    // ── @chunk directive ────────────────────────────────────────────────

    #[tokio::test]
    async fn chunk_directive_splits_file_into_chunks() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("big.txt");
        // 10 bytes of data, chunk at 4 bytes → 3 chunks
        std::fs::write(&input, "0123456789").expect("write input");

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::Path(
                input.to_string_lossy().to_string(),
            ))),
            operations: vec![(
                PipeOp::Safe,
                Destination::Directive(DirectiveFlow {
                    name: "chunk".to_string(),
                    arguments: vec![Expression::Literal(Literal::String("4".to_string()))],
                    alias: None,
                }),
            )],
            on_fail: None,
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        let result = runtime.execute_flow(&flow).await.expect("should execute");

        match result {
            Value::List(chunks) => {
                assert_eq!(chunks.len(), 3);
                assert_eq!(chunks[0].as_string(), "0123");
                assert_eq!(chunks[1].as_string(), "4567");
                assert_eq!(chunks[2].as_string(), "89");
            }
            _ => panic!("expected list of chunks, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn chunk_directive_rejects_zero_size() {
        let dir = tempdir().expect("tempdir");
        let input = dir.path().join("data.txt");
        std::fs::write(&input, "abc").expect("write input");

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::Path(
                input.to_string_lossy().to_string(),
            ))),
            operations: vec![(
                PipeOp::Safe,
                Destination::Directive(DirectiveFlow {
                    name: "chunk".to_string(),
                    arguments: vec![Expression::Literal(Literal::String("0".to_string()))],
                    alias: None,
                }),
            )],
            on_fail: None,
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        let result = runtime.execute_flow(&flow).await;
        assert!(result.is_err(), "chunk with size 0 should fail");
    }

    // ── Builtin functions ───────────────────────────────────────────────

    #[tokio::test]
    async fn concat_function_joins_values() {
        let mut runtime = Runtime::new();
        let result = runtime
            .call_function("concat", vec![
                Value::String("hello".to_string()),
                Value::String(" ".to_string()),
                Value::String("world".to_string()),
            ])
            .await
            .expect("concat should succeed");
        assert_eq!(result.as_string(), "hello world");
    }

    #[tokio::test]
    async fn exists_function_returns_true_for_existing_file() {
        let dir = tempdir().expect("tempdir");
        let file = dir.path().join("present.txt");
        std::fs::write(&file, "x").expect("write");

        let mut runtime = Runtime::new();
        let result = runtime
            .call_function("exists", vec![Value::String(file.to_string_lossy().to_string())])
            .await
            .expect("exists should succeed");
        assert!(matches!(result, Value::Boolean(true)));
    }

    #[tokio::test]
    async fn exists_function_returns_false_for_missing_file() {
        let mut runtime = Runtime::new();
        let result = runtime
            .call_function("exists", vec![Value::String("/nonexistent/file.txt".to_string())])
            .await
            .expect("exists should succeed");
        assert!(matches!(result, Value::Boolean(false)));
    }

    #[tokio::test]
    async fn print_function_returns_argument_as_string() {
        let mut runtime = Runtime::new();
        let result = runtime
            .call_function("print", vec![Value::Number(42.0)])
            .await
            .expect("print should succeed");
        assert_eq!(result.as_string(), "42");
    }

    // ── Expression evaluation at runtime ────────────────────────────────

    #[tokio::test]
    async fn binary_op_arithmetic_in_expression() {
        let mut runtime = Runtime::new();
        // 2 + 3 * 4 should be 14 (precedence: * before +)
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Number(2.0))),
            "+".to_string(),
            Box::new(Expression::BinaryOp(
                Box::new(Expression::Literal(Literal::Number(3.0))),
                "*".to_string(),
                Box::new(Expression::Literal(Literal::Number(4.0))),
            )),
        );
        let result = runtime.eval_expression(&expr).await.expect("should eval");
        assert!(matches!(result, Value::Number(n) if n == 14.0));
    }

    #[tokio::test]
    async fn binary_op_string_concatenation() {
        let mut runtime = Runtime::new();
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::String("foo".to_string()))),
            "+".to_string(),
            Box::new(Expression::Literal(Literal::String("bar".to_string()))),
        );
        let result = runtime.eval_expression(&expr).await.expect("should eval");
        assert_eq!(result.as_string(), "foobar");
    }

    #[tokio::test]
    async fn binary_op_division_by_zero_errors() {
        let mut runtime = Runtime::new();
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Number(10.0))),
            "/".to_string(),
            Box::new(Expression::Literal(Literal::Number(0.0))),
        );
        let result = runtime.eval_expression(&expr).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Division by zero"));
    }

    #[tokio::test]
    async fn binary_op_comparison_operators() {
        let mut runtime = Runtime::new();
        // 5 > 3
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Number(5.0))),
            ">".to_string(),
            Box::new(Expression::Literal(Literal::Number(3.0))),
        );
        let result = runtime.eval_expression(&expr).await.expect("should eval");
        assert!(matches!(result, Value::Boolean(true)));

        // 5 <= 3
        let expr2 = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Number(5.0))),
            "<=".to_string(),
            Box::new(Expression::Literal(Literal::Number(3.0))),
        );
        let result2 = runtime.eval_expression(&expr2).await.expect("should eval");
        assert!(matches!(result2, Value::Boolean(false)));
    }

    #[tokio::test]
    async fn logical_and_short_circuits_on_false() {
        let mut runtime = Runtime::new();
        // false && <undefined_var> should NOT error — short-circuit
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Boolean(false))),
            "&&".to_string(),
            Box::new(Expression::Identifier("undefined_thing".to_string())),
        );
        let result = runtime.eval_expression(&expr).await.expect("should short-circuit");
        assert!(matches!(result, Value::Boolean(false)));
    }

    #[tokio::test]
    async fn logical_or_short_circuits_on_true() {
        let mut runtime = Runtime::new();
        // true || <undefined_var> should NOT error — short-circuit
        let expr = Expression::BinaryOp(
            Box::new(Expression::Literal(Literal::Boolean(true))),
            "||".to_string(),
            Box::new(Expression::Identifier("undefined_thing".to_string())),
        );
        let result = runtime.eval_expression(&expr).await.expect("should short-circuit");
        assert!(matches!(result, Value::Boolean(true)));
    }

    #[tokio::test]
    async fn unary_not_on_boolean() {
        let mut runtime = Runtime::new();
        let expr = Expression::UnaryOp(
            "!".to_string(),
            Box::new(Expression::Literal(Literal::Boolean(true))),
        );
        let result = runtime.eval_expression(&expr).await.expect("should eval");
        assert!(matches!(result, Value::Boolean(false)));
    }

    #[tokio::test]
    async fn unary_not_on_non_boolean_errors() {
        let mut runtime = Runtime::new();
        let expr = Expression::UnaryOp(
            "!".to_string(),
            Box::new(Expression::Literal(Literal::Number(42.0))),
        );
        let result = runtime.eval_expression(&expr).await;
        assert!(result.is_err());
    }

    // ── is_truthy edge cases ────────────────────────────────────────────

    #[test]
    fn is_truthy_edge_cases() {
        let runtime = Runtime::new();
        assert!(!runtime.is_truthy(&Value::Null));
        assert!(!runtime.is_truthy(&Value::Boolean(false)));
        assert!(!runtime.is_truthy(&Value::Number(0.0)));
        assert!(runtime.is_truthy(&Value::Boolean(true)));
        assert!(runtime.is_truthy(&Value::Number(1.0)));
        assert!(runtime.is_truthy(&Value::Number(-1.0)));
        assert!(runtime.is_truthy(&Value::String("hello".to_string())));
        assert!(!runtime.is_truthy(&Value::String("".to_string())));
    }

    // ── Multi-stage pipeline ────────────────────────────────────────────

    #[tokio::test]
    async fn full_pipeline_read_parse_filter_write() {
        let dir = tempdir().expect("tempdir");
        let csv_input = dir.path().join("products.csv");
        let csv_output = dir.path().join("expensive.csv");

        std::fs::write(&csv_input, "name,price\nWidget,500\nGadget,1500\nThing,200\n")
            .expect("write csv");

        // @read >> @csv.parse >> filter(lambda) >> "output"
        let flow = PipeFlow {
            source: Source::Directive(DirectiveFlow {
                name: "read".to_string(),
                arguments: vec![Expression::Literal(Literal::Path(
                    csv_input.to_string_lossy().to_string(),
                ))],
                alias: None,
            }),
            operations: vec![
                (PipeOp::Safe, Destination::Directive(DirectiveFlow {
                    name: "csv.parse".to_string(),
                    arguments: vec![],
                    alias: Some("data".to_string()),
                })),
                (PipeOp::Safe, Destination::Directive(DirectiveFlow {
                    name: "filter".to_string(),
                    arguments: vec![Expression::Lambda(Lambda {
                        param: "row".to_string(),
                        body: Box::new(Expression::BinaryOp(
                            Box::new(Expression::MemberAccess(vec!["row".to_string(), "price".to_string()])),
                            ">".to_string(),
                            Box::new(Expression::Literal(Literal::Number(1000.0))),
                        )),
                    })],
                    alias: None,
                })),
                (PipeOp::Safe, Destination::Expression(Expression::Literal(Literal::Path(
                    csv_output.to_string_lossy().to_string(),
                )))),
            ],
            on_fail: None,
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        runtime.execute_flow(&flow).await.expect("pipeline should succeed");

        let output = std::fs::read_to_string(&csv_output).expect("read output");
        assert!(output.contains("Gadget"));
        assert!(!output.contains("Widget"));
        assert!(!output.contains("Thing"));
    }

    // ── @filter with empty list ─────────────────────────────────────────

    #[tokio::test]
    async fn filter_with_empty_list_returns_empty_list() {
        let mut runtime = Runtime::new();
        let result = runtime
            .call_filter(vec![
                Value::List(vec![]),
                Value::Lambda(Lambda {
                    param: "x".to_string(),
                    body: Box::new(Expression::Literal(Literal::Boolean(true))),
                }),
            ])
            .await
            .expect("filter should succeed");
        assert!(matches!(result, Value::List(items) if items.is_empty()));
    }

    #[tokio::test]
    async fn filter_with_no_matches_returns_empty_list() {
        let mut runtime = Runtime::new();
        let mut row = std::collections::HashMap::new();
        row.insert("val".to_string(), Value::Number(5.0));
        let result = runtime
            .call_filter(vec![
                Value::List(vec![Value::Record(row)]),
                Value::Lambda(Lambda {
                    param: "x".to_string(),
                    body: Box::new(Expression::BinaryOp(
                        Box::new(Expression::MemberAccess(vec!["x".to_string(), "val".to_string()])),
                        ">".to_string(),
                        Box::new(Expression::Literal(Literal::Number(100.0))),
                    )),
                }),
            ])
            .await
            .expect("filter should succeed");
        assert!(matches!(result, Value::List(items) if items.is_empty()));
    }

    // ── on_fail error propagation ───────────────────────────────────────

    #[tokio::test]
    async fn on_fail_binds_error_message_to_alias() {
        let dir = tempdir().expect("tempdir");
        let error_log = dir.path().join("err.txt");

        // Source reads a non-existent file → triggers on_fail
        // on_fail as e >> e >> "err.txt"
        let flow = PipeFlow {
            source: Source::Directive(DirectiveFlow {
                name: "read".to_string(),
                arguments: vec![Expression::Literal(Literal::Path(
                    "/definitely/not/a/real/file.txt".to_string(),
                ))],
                alias: None,
            }),
            operations: vec![(
                PipeOp::Safe,
                Destination::Expression(Expression::Literal(Literal::Path(
                    "unused.txt".to_string(),
                ))),
            )],
            on_fail: Some(OnFail {
                alias: Some("e".to_string()),
                handler: Box::new(FlowOrBranch::Flow(PipeFlow {
                    source: Source::Expression(Expression::Identifier("e".to_string())),
                    operations: vec![(
                        PipeOp::Safe,
                        Destination::Expression(Expression::Literal(Literal::Path(
                            error_log.to_string_lossy().to_string(),
                        ))),
                    )],
                    on_fail: None,
                })),
            }),
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        runtime.execute_flow(&flow).await.expect("on_fail should handle gracefully");

        let err_content = std::fs::read_to_string(&error_log).expect("error log should exist");
        assert!(err_content.contains("Failed to read"), "error message should mention the failure: {}", err_content);
    }

    // ── Branch with multiple file outputs ───────────────────────────────

    #[tokio::test]
    async fn branch_fans_out_to_multiple_file_destinations() {
        let dir = tempdir().expect("tempdir");
        let out_a = dir.path().join("a.txt");
        let out_b = dir.path().join("b.txt");

        let flow = PipeFlow {
            source: Source::Expression(Expression::Literal(Literal::String("shared data".to_string()))),
            operations: vec![(
                PipeOp::Safe,
                Destination::Branch(Branch {
                    flows: vec![
                        PipeFlow {
                            source: Source::Expression(Expression::Literal(Literal::Path(
                                out_a.to_string_lossy().to_string(),
                            ))),
                            operations: vec![],
                            on_fail: None,
                        },
                        PipeFlow {
                            source: Source::Expression(Expression::Literal(Literal::Path(
                                out_b.to_string_lossy().to_string(),
                            ))),
                            operations: vec![],
                            on_fail: None,
                        },
                    ],
                }),
            )],
            on_fail: None,
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        runtime.execute_flow(&flow).await.expect("branch should succeed");

        let a_content = std::fs::read_to_string(&out_a).expect("a.txt should exist");
        let b_content = std::fs::read_to_string(&out_b).expect("b.txt should exist");
        assert!(a_content.contains("shared data"));
        assert!(b_content.contains("shared data"));
    }

    // ── User-defined function called via pipe ───────────────────────────

    #[tokio::test]
    async fn user_function_called_in_pipeline() {
        let dir = tempdir().expect("tempdir");
        let out = dir.path().join("result.txt");

        // Define: shout(x) => x + "!"
        // Then: s"hello" >> shout >> "result.txt"
        let program = Program {
            statements: vec![
                Statement::Function(FunctionDef {
                    name: "shout".to_string(),
                    parameters: vec!["x".to_string()],
                    body: FlowOrBranch::Flow(PipeFlow {
                        source: Source::Expression(Expression::BinaryOp(
                            Box::new(Expression::Identifier("x".to_string())),
                            "+".to_string(),
                            Box::new(Expression::Literal(Literal::String("!".to_string()))),
                        )),
                        operations: vec![],
                        on_fail: None,
                    }),
                }),
                Statement::Pipe(PipeFlow {
                    source: Source::Expression(Expression::Literal(Literal::String("hello".to_string()))),
                    operations: vec![
                        (PipeOp::Safe, Destination::FunctionCall(FunctionCall {
                            name: "shout".to_string(),
                            arguments: vec![],
                            alias: None,
                        })),
                        (PipeOp::Safe, Destination::Expression(Expression::Literal(Literal::Path(
                            out.to_string_lossy().to_string(),
                        )))),
                    ],
                    on_fail: None,
                }),
            ],
        };
        let mut runtime = Runtime::new().with_script_dir(dir.path().to_str().unwrap());
        runtime.execute(&program).await.expect("should execute");

        let content = std::fs::read_to_string(&out).expect("should read output");
        assert!(content.contains("hello!"));
    }

    // ── @map over list ──────────────────────────────────────────────────

    #[tokio::test]
    async fn map_transforms_each_item_in_list() {
        let mut runtime = Runtime::new();
        let result = runtime
            .call_map(vec![
                Value::List(vec![
                    Value::Number(1.0),
                    Value::Number(2.0),
                    Value::Number(3.0),
                ]),
                Value::Lambda(Lambda {
                    param: "x".to_string(),
                    body: Box::new(Expression::BinaryOp(
                        Box::new(Expression::Identifier("x".to_string())),
                        "*".to_string(),
                        Box::new(Expression::Literal(Literal::Number(10.0))),
                    )),
                }),
            ])
            .await
            .expect("map should succeed");

        match result {
            Value::List(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(&items[0], Value::Number(n) if *n == 10.0));
                assert!(matches!(&items[1], Value::Number(n) if *n == 20.0));
                assert!(matches!(&items[2], Value::Number(n) if *n == 30.0));
            }
            _ => panic!("expected list, got {:?}", result),
        }
    }
}
