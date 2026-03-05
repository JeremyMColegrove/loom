use crate::ast::*;
use crate::runtime::env::Value;
use crate::runtime::Runtime;

impl Runtime {
    pub(crate) fn eval_directive<'a>(&'a mut self, directive: &'a DirectiveFlow) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
        self.eval_directive_with_pipe(directive, Value::Null)
    }

    pub(crate) fn eval_directive_with_pipe<'a>(&'a mut self, directive: &'a DirectiveFlow, pipe_val: Value) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
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
}
