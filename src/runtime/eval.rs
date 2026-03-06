use crate::ast::*;
use crate::runtime::Runtime;
use crate::runtime::env::Value;
use crate::runtime::error::{RuntimeError, RuntimeResult};

impl Runtime {
    pub(crate) fn eval_expression<'a>(
        &'a mut self,
        expr: &'a Expression,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Value>> + 'a>> {
        Box::pin(async move {
            match expr {
                Expression::Literal(lit) => match lit {
                    Literal::Path(s) => Ok(Value::Path(s.clone())),
                    Literal::String(s) => Ok(Value::String(s.clone())),
                    Literal::Number(n) => Ok(Value::Number(*n)),
                    Literal::Boolean(b) => Ok(Value::Boolean(*b)),
                },
                Expression::Identifier(name) => {
                    self.env.get(name).cloned().ok_or_else(|| {
                        RuntimeError::message(format!("Undefined variable: {}", name))
                    })
                }
                Expression::ObjectLiteral(entries) => {
                    let mut map = std::collections::HashMap::new();
                    for (key, value_expr) in entries {
                        map.insert(
                            key.as_map_key().to_string(),
                            self.eval_expression(value_expr).await?,
                        );
                    }
                    Ok(Value::Record(map))
                }
                Expression::MemberAccess(parts) => {
                    if parts.is_empty() {
                        return Err(RuntimeError::message("Invalid member access"));
                    }
                    let root = &parts[0];
                    let mut value = self.env.get(root).cloned().ok_or_else(|| {
                        RuntimeError::message(format!("Undefined variable: {}", root))
                    })?;
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
                            _ => Err(RuntimeError::message(format!(
                                "Cannot negate non-boolean: {:?}",
                                val
                            ))),
                        },
                        _ => Err(RuntimeError::message(format!(
                            "Unknown unary operator: {}",
                            op
                        ))),
                    }
                }
                Expression::FunctionCall(call) => {
                    let mut args = Vec::new();
                    for arg in &call.arguments {
                        args.push(self.eval_expression(arg).await?);
                    }
                    self.call_function(&call.name, args).await
                }
                Expression::SecretCall(call) => {
                    let (args, named_args) = self
                        .eval_call_arguments(&call.arguments, &call.named_arguments)
                        .await?;
                    let value = self.resolve_secret_from_call_args(&args, &named_args)?;
                    Ok(Value::String(value))
                }
                Expression::Lambda(lambda) => Ok(Value::Lambda(lambda.clone())),
            }
        })
    }

    pub(crate) fn eval_binary_op(
        &self,
        left: &Value,
        op: &str,
        right: &Value,
    ) -> RuntimeResult<Value> {
        let to_number = |v: &Value| -> Option<f64> {
            match v {
                Value::Number(n) => Some(*n),
                Value::String(s) => s.trim().parse::<f64>().ok(),
                Value::Path(p) => p.trim().parse::<f64>().ok(),
                _ => None,
            }
        };

        match op {
            "+" => match (left, right) {
                (Value::Number(a), Value::Number(b)) => Ok(Value::Number(a + b)),
                (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
                _ => Ok(Value::String(format!(
                    "{}{}",
                    left.as_string(),
                    right.as_string()
                ))),
            },
            "-" => match (left, right) {
                (Value::Number(a), Value::Number(b)) => Ok(Value::Number(a - b)),
                _ => Err(RuntimeError::message("Cannot subtract non-numbers")),
            },
            "*" => match (left, right) {
                (Value::Number(a), Value::Number(b)) => Ok(Value::Number(a * b)),
                _ => Err(RuntimeError::message("Cannot multiply non-numbers")),
            },
            "/" => match (left, right) {
                (Value::Number(a), Value::Number(b)) => {
                    if *b == 0.0 {
                        Err(RuntimeError::message("Division by zero"))
                    } else {
                        Ok(Value::Number(a / b))
                    }
                }
                _ => Err(RuntimeError::message("Cannot divide non-numbers")),
            },
            "==" => Ok(Value::Boolean(left.as_string() == right.as_string())),
            "!=" => Ok(Value::Boolean(left.as_string() != right.as_string())),
            ">" => match (to_number(left), to_number(right)) {
                (Some(a), Some(b)) => Ok(Value::Boolean(a > b)),
                _ => Err(RuntimeError::message("Cannot compare '>' for non-numbers")),
            },
            "<" => match (to_number(left), to_number(right)) {
                (Some(a), Some(b)) => Ok(Value::Boolean(a < b)),
                _ => Err(RuntimeError::message("Cannot compare '<' for non-numbers")),
            },
            ">=" => match (to_number(left), to_number(right)) {
                (Some(a), Some(b)) => Ok(Value::Boolean(a >= b)),
                _ => Err(RuntimeError::message("Cannot compare '>=' for non-numbers")),
            },
            "<=" => match (to_number(left), to_number(right)) {
                (Some(a), Some(b)) => Ok(Value::Boolean(a <= b)),
                _ => Err(RuntimeError::message("Cannot compare '<=' for non-numbers")),
            },
            _ => Err(RuntimeError::message(format!("Unknown operator: {}", op))),
        }
    }

    pub(crate) fn is_truthy(&self, value: &Value) -> bool {
        match value {
            Value::Boolean(b) => *b,
            Value::Number(n) => *n != 0.0,
            Value::Null => false,
            _ => !value.as_string().is_empty(),
        }
    }
}
