use crate::ast::*;
use crate::runtime::env::Value;
use crate::runtime::Runtime;

impl Runtime {
    pub(crate) fn eval_function_call<'a>(&'a mut self, call: &'a FunctionCall) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        Box::pin(async move {
            let mut args = Vec::new();
            for arg in &call.arguments {
                args.push(self.eval_expression(arg).await?);
            }
            self.call_function(&call.name, args).await
        })
    }

    pub(crate) fn eval_function_call_with_pipe<'a>(&'a mut self, call: &'a FunctionCall, pipe_val: Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
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

    pub(crate) fn resolve_function(&self, name: &str) -> Option<FunctionDef> {
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

    pub(crate) fn call_function<'a>(&'a mut self, name: &'a str, args: Vec<Value>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
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

    pub(crate) fn call_filter<'a>(&'a mut self, args: Vec<Value>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
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

    pub(crate) fn call_map<'a>(&'a mut self, args: Vec<Value>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
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
}
