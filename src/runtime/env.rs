use crate::ast::{FunctionDef, Lambda};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum Value {
    Path(String),
    String(String),
    Number(f64),
    Boolean(bool),
    List(Vec<Value>),
    Record(HashMap<String, Value>),
    Lambda(Lambda),
    Function(Arc<FunctionDef>),
    Null,
}

impl Value {
    pub fn as_string(&self) -> String {
        match self {
            Value::Path(p) => p.clone(),
            Value::String(s) => s.clone(),
            Value::Number(n) => {
                if *n == (*n as i64) as f64 {
                    format!("{}", *n as i64)
                } else {
                    format!("{}", n)
                }
            }
            Value::Boolean(b) => format!("{}", b),
            Value::Null => "null".to_string(),
            Value::List(items) => {
                let parts: Vec<String> = items.iter().map(|v| v.as_string()).collect();
                format!("[{}]", parts.join(", "))
            }
            Value::Record(map) => {
                let parts: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v.as_string()))
                    .collect();
                format!("{{{}}}", parts.join(", "))
            }
            Value::Lambda(lambda) => format!("<lambda {}>", lambda.param),
            Value::Function(func) => format!("<function {}>", func.name),
        }
    }

    pub fn as_path(&self) -> Option<&str> {
        match self {
            Value::Path(p) => Some(p),
            Value::String(s) => Some(s),
            Value::Record(map) => {
                if let Some(Value::Path(p)) = map.get("path") {
                    Some(p)
                } else if let Some(Value::String(s)) = map.get("path") {
                    Some(s)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_member(&self, member: &str) -> Result<Value, String> {
        match self {
            Value::Path(p) => match member {
                "name" => {
                    let name = std::path::Path::new(p)
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_string();
                    Ok(Value::String(name))
                }
                _ => Err(format!("No member '{}' on path", member)),
            },
            Value::Record(map) => {
                if let Some(val) = map.get(member) {
                    return Ok(val.clone());
                }

                map.iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(member))
                    .map(|(_, v)| v.clone())
                    .ok_or_else(|| format!("No member '{}' found on record", member))
            }
            Value::String(s) => match member {
                "length" => Ok(Value::Number(s.len() as f64)),
                _ => Err(format!("No member '{}' on string", member)),
            },
            _ => Err(format!("Cannot access member '{}' on {:?}", member, self)),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

pub struct Environment {
    scopes: Vec<HashMap<String, Value>>,
}

impl Default for Environment {
    fn default() -> Self {
        Self::new()
    }
}

impl Environment {
    pub fn new() -> Self {
        Self {
            scopes: vec![HashMap::new()],
        }
    }

    pub fn push_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    pub fn pop_scope(&mut self) {
        if self.scopes.len() > 1 {
            self.scopes.pop();
        }
    }

    pub fn set(&mut self, name: &str, value: Value) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name.to_string(), value);
        }
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        for scope in self.scopes.iter().rev() {
            if let Some(val) = scope.get(name) {
                return Some(val);
            }
        }
        None
    }

    pub fn register_function(&mut self, func: FunctionDef) {
        self.set(&func.name.clone(), Value::Function(Arc::new(func)));
    }

    pub fn get_function(&self, name: &str) -> Option<Arc<FunctionDef>> {
        match self.get(name) {
            Some(Value::Function(f)) => Some(Arc::clone(f)),
            _ => None,
        }
    }

    /// Extracts the globals from the base scope (used for module exports)
    pub fn extract_globals(&self) -> HashMap<String, Value> {
        self.scopes.first().cloned().unwrap_or_default()
    }
}


include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/unit/runtime_env_tests.rs"));
