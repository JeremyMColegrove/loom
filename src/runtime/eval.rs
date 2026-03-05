use crate::ast::*;
use crate::runtime::env::Value;
use crate::runtime::Runtime;

impl Runtime {
    pub(crate) fn eval_expression<'a>(&'a mut self, expr: &'a Expression) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + 'a>> {
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

    pub(crate) fn eval_binary_op(&self, left: &Value, op: &str, right: &Value) -> Result<Value, String> {
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

    pub(crate) fn is_truthy(&self, value: &Value) -> bool {
        match value {
            Value::Boolean(b) => *b,
            Value::Number(n) => *n != 0.0,
            Value::Null => false,
            _ => !value.as_string().is_empty(),
        }
    }
}
