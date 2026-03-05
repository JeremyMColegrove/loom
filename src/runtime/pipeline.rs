use crate::ast::*;
use crate::runtime::Runtime;
use crate::runtime::env::Value;
use log::warn;

impl Runtime {
    pub fn execute_flow<'a>(
        &'a mut self,
        flow: &'a PipeFlow,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
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

            self.run_flow_operations(flow, current_value, atomic_started_here)
                .await
        })
    }

    pub(crate) fn run_flow_operations<'a>(
        &'a mut self,
        flow: &'a PipeFlow,
        mut current_value: Value,
        atomic_started_here_initial: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
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
                                warn!("force pipe ignored downstream error: {}", e);
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

    pub(crate) fn eval_destination<'a>(
        &'a mut self,
        op: &'a PipeOp,
        dest: &'a Destination,
        pipe_val: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
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
                            if self.callable_sinks.contains(name)
                                && self.env.get_function(name).is_some()
                            {
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

    pub(crate) fn handle_on_fail<'a>(
        &'a mut self,
        on_fail: &'a OnFail,
        error: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
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

    pub(crate) fn execute_branch<'a>(
        &'a mut self,
        branch: &'a Branch,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            let mut last_value = Value::Null;
            for item in &branch.items {
                if let BranchItem::Flow(flow) = item {
                    last_value = self.execute_flow(flow).await?;
                }
            }
            Ok(last_value)
        })
    }

    pub(crate) fn execute_branch_with_input<'a>(
        &'a mut self,
        branch: &'a Branch,
        input: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            let mut last_value = input.clone();
            for item in &branch.items {
                let BranchItem::Flow(flow) = item else {
                    continue;
                };
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

    pub(crate) fn run_branch_flow_with_input<'a>(
        &'a mut self,
        flow: &'a PipeFlow,
        input: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            if flow.operations.is_empty() {
                if let Source::Expression(Expression::Literal(Literal::Path(path))) = &flow.source {
                    return self.write_or_move_path(&PipeOp::Safe, path, &input);
                }
            }
            let mut current_value = self
                .eval_branch_source_with_input(&flow.source, input)
                .await?;
            for (op, dest) in &flow.operations {
                current_value = self.eval_destination(op, dest, current_value).await?;
            }
            Ok(current_value)
        })
    }

    pub(crate) fn eval_branch_source_with_input<'a>(
        &'a mut self,
        source: &'a Source,
        input: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            match source {
                Source::Directive(directive) => {
                    self.eval_directive_with_pipe(directive, input).await
                }
                Source::FunctionCall(call) => self.eval_function_call_with_pipe(call, input).await,
                Source::Expression(expr) => match expr {
                    Expression::Literal(Literal::Path(path)) => Ok(Value::Path(path.clone())),
                    Expression::Identifier(name) if name == "_" => Ok(input),
                    _ => self.eval_expression(expr).await,
                },
            }
        })
    }

    pub(crate) fn eval_source<'a>(
        &'a mut self,
        source: &'a Source,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            match source {
                Source::Directive(directive) => self.eval_directive(directive).await,
                Source::FunctionCall(call) => self.eval_function_call(call).await,
                Source::Expression(expr) => match expr {
                    Expression::Literal(Literal::Path(path)) => Ok(Value::Path(path.clone())),
                    _ => self.eval_expression(expr).await,
                },
            }
        })
    }
}
