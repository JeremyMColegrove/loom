use crate::runtime::Runtime;
use crate::runtime::env::Value;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use std::collections::HashMap;
use std::path::PathBuf;

impl Runtime {
    pub(crate) fn resolve_secret_from_call_args(
        &self,
        args: &[Value],
        named_args: &HashMap<String, Value>,
    ) -> RuntimeResult<String> {
        if !named_args.is_empty() {
            return Err(RuntimeError::message(
                "@secret does not support named arguments",
            ));
        }
        if args.len() != 1 {
            return Err(RuntimeError::message(
                "@secret expects exactly one argument",
            ));
        }

        let key = args[0].as_string();
        self.resolve_secret(&key)
    }

    pub(crate) fn resolve_secret(&self, key: &str) -> RuntimeResult<String> {
        let dotenv_path = self.resolve_dotenv_path();
        if let Some(value) = self.read_dotenv_key(&dotenv_path, key) {
            return Ok(value);
        }
        if let Ok(value) = std::env::var(key) {
            return Ok(value);
        }
        Err(RuntimeError::message(format!(
            "Missing secret '{}': not found in .env ({}) or environment variables",
            key,
            dotenv_path.display()
        )))
    }

    fn read_dotenv_key(&self, dotenv_path: &std::path::Path, key: &str) -> Option<String> {
        if !dotenv_path.is_file() {
            return None;
        }
        let contents = std::fs::read_to_string(dotenv_path).ok()?;
        parse_dotenv_contents(&contents).remove(key)
    }

    fn resolve_dotenv_path(&self) -> PathBuf {
        let base_dir = self
            .script_dir
            .as_ref()
            .map(PathBuf::from)
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."));
        base_dir.join(".env")
    }
}

fn parse_dotenv_contents(contents: &str) -> HashMap<String, String> {
    let mut values = HashMap::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let without_export = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        let Some((raw_key, raw_value)) = without_export.split_once('=') else {
            continue;
        };
        let key = raw_key.trim();
        if key.is_empty() {
            continue;
        }
        values.insert(key.to_string(), parse_dotenv_value(raw_value.trim()));
    }
    values
}

fn parse_dotenv_value(raw: &str) -> String {
    if raw.len() >= 2 && raw.starts_with('"') && raw.ends_with('"') {
        return unescape_quoted_dotenv_value(&raw[1..raw.len() - 1]);
    }
    if raw.len() >= 2 && raw.starts_with('\'') && raw.ends_with('\'') {
        return raw[1..raw.len() - 1].to_string();
    }
    raw.to_string()
}

fn unescape_quoted_dotenv_value(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();

    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }

    out
}
