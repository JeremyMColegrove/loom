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

#[cfg(test)]
mod tests {
    use super::*;

    // ── Value::get_member ────────────────────────────────────────────────

    #[test]
    pub(crate) fn get_member_record_existing_key() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::String("Alice".to_string()));
        let record = Value::Record(map);
        let result = record.get_member("name").unwrap();
        assert!(matches!(result, Value::String(s) if s == "Alice"));
    }

    #[test]
    pub(crate) fn get_member_record_missing_key_returns_error() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::String("Alice".to_string()));
        let record = Value::Record(map);
        let result = record.get_member("age");
        assert!(result.is_err());
    }

    #[test]
    pub(crate) fn get_member_record_case_insensitive_fallback() {
        let mut map = HashMap::new();
        map.insert("Name".to_string(), Value::String("Alice".to_string()));
        let record = Value::Record(map);
        let result = record.get_member("name").unwrap();
        assert!(matches!(result, Value::String(s) if s == "Alice"));
    }

    #[test]
    pub(crate) fn get_member_path_name() {
        let val = Value::Path("/home/user/document.txt".to_string());
        let result = val.get_member("name").unwrap();
        assert!(matches!(result, Value::String(s) if s == "document.txt"));
    }

    #[test]
    pub(crate) fn get_member_path_unknown_returns_error() {
        let val = Value::Path("/home/user/document.txt".to_string());
        let result = val.get_member("size");
        assert!(result.is_err());
    }

    #[test]
    pub(crate) fn get_member_string_length() {
        let val = Value::String("hello".to_string());
        let result = val.get_member("length").unwrap();
        assert!(matches!(result, Value::Number(n) if n == 5.0));
    }

    #[test]
    pub(crate) fn get_member_string_unknown_returns_error() {
        let val = Value::String("hello".to_string());
        let result = val.get_member("name");
        assert!(result.is_err());
    }

    #[test]
    pub(crate) fn get_member_number_returns_error() {
        let val = Value::Number(42.0);
        let result = val.get_member("anything");
        assert!(result.is_err());
    }

    #[test]
    pub(crate) fn get_member_nested_record() {
        let mut inner = HashMap::new();
        inner.insert("ext".to_string(), Value::String("csv".to_string()));
        let mut outer = HashMap::new();
        outer.insert("file".to_string(), Value::Record(inner));
        let record = Value::Record(outer);
        let file = record.get_member("file").unwrap();
        let ext = file.get_member("ext").unwrap();
        assert!(matches!(ext, Value::String(s) if s == "csv"));
    }

    // ── Value::as_string ────────────────────────────────────────────────

    #[test]
    pub(crate) fn as_string_variants() {
        assert_eq!(Value::Path("/a/b".to_string()).as_string(), "/a/b");
        assert_eq!(Value::String("hello".to_string()).as_string(), "hello");
        assert_eq!(Value::Number(42.0).as_string(), "42");
        assert_eq!(Value::Number(2.5).as_string(), "2.5");
        assert_eq!(Value::Boolean(true).as_string(), "true");
        assert_eq!(Value::Boolean(false).as_string(), "false");
        assert_eq!(Value::Null.as_string(), "null");
    }

    #[test]
    pub(crate) fn as_string_integer_format() {
        assert_eq!(Value::Number(100.0).as_string(), "100");
        assert_eq!(Value::Number(-7.0).as_string(), "-7");
    }

    #[test]
    pub(crate) fn as_string_list() {
        let list = Value::List(vec![
            Value::Number(1.0),
            Value::Number(2.0),
            Value::Number(3.0),
        ]);
        assert_eq!(list.as_string(), "[1, 2, 3]");
    }

    // ── Value::as_path ──────────────────────────────────────────────────

    #[test]
    pub(crate) fn as_path_returns_some_for_path_and_string() {
        assert_eq!(Value::Path("/a".to_string()).as_path(), Some("/a"));
        assert_eq!(Value::String("b".to_string()).as_path(), Some("b"));
    }

    #[test]
    pub(crate) fn as_path_returns_some_for_record_with_path_key() {
        let mut map = HashMap::new();
        map.insert(
            "path".to_string(),
            Value::String("/data/file.csv".to_string()),
        );
        let record = Value::Record(map);
        assert_eq!(record.as_path(), Some("/data/file.csv"));
    }

    #[test]
    pub(crate) fn as_path_returns_none_for_number() {
        assert!(Value::Number(42.0).as_path().is_none());
    }

    // ── Environment Scoping ─────────────────────────────────────────────

    #[test]
    pub(crate) fn env_set_and_get() {
        let mut env = Environment::new();
        env.set("x", Value::Number(42.0));
        let val = env.get("x").unwrap();
        assert!(matches!(val, Value::Number(n) if *n == 42.0));
    }

    #[test]
    pub(crate) fn env_get_undefined_returns_none() {
        let env = Environment::new();
        assert!(env.get("x").is_none());
    }

    #[test]
    pub(crate) fn env_inner_scope_shadows_outer() {
        let mut env = Environment::new();
        env.set("x", Value::Number(1.0));
        env.push_scope();
        env.set("x", Value::Number(2.0));
        let val = env.get("x").unwrap();
        assert!(matches!(val, Value::Number(n) if *n == 2.0));
    }

    #[test]
    pub(crate) fn env_pop_scope_restores_outer_value() {
        let mut env = Environment::new();
        env.set("x", Value::Number(1.0));
        env.push_scope();
        env.set("x", Value::Number(2.0));
        env.pop_scope();
        let val = env.get("x").unwrap();
        assert!(matches!(val, Value::Number(n) if *n == 1.0));
    }

    #[test]
    pub(crate) fn env_pop_scope_does_not_remove_base_scope() {
        let mut env = Environment::new();
        env.set("x", Value::Number(1.0));
        env.pop_scope();
        let val = env.get("x").unwrap();
        assert!(matches!(val, Value::Number(n) if *n == 1.0));
    }

    #[test]
    pub(crate) fn env_inner_scope_reads_outer_variables() {
        let mut env = Environment::new();
        env.set("x", Value::Number(1.0));
        env.push_scope();
        let val = env.get("x").unwrap();
        assert!(matches!(val, Value::Number(n) if *n == 1.0));
    }

    #[test]
    pub(crate) fn env_register_and_get_function() {
        let mut env = Environment::new();
        let func = FunctionDef {
            comments: vec![],
            name: "add".to_string(),
            parameters: vec!["a".to_string(), "b".to_string()],
            body: crate::ast::FlowOrBranch::Flow(crate::ast::PipeFlow {
                comments: vec![],
                source: crate::ast::Source::Expression(crate::ast::Expression::Identifier(
                    "a".to_string(),
                )),
                operations: vec![],
                on_fail: None,
                span: crate::ast::Span::default(),
            }),
            span: crate::ast::Span::default(),
        };
        env.register_function(func);
        assert!(env.get_function("add").is_some());
        assert!(env.get_function("nonexistent").is_none());
    }

    #[test]
    pub(crate) fn env_extract_globals() {
        let mut env = Environment::new();
        env.set("global_var", Value::Number(42.0));
        env.push_scope();
        env.set("local_var", Value::Number(99.0));
        let globals = env.extract_globals();
        assert!(globals.contains_key("global_var"));
        assert!(!globals.contains_key("local_var"));
    }
}
