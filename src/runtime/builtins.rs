use crate::builtin_spec::{
    DIRECTIVE_ATOMIC, DIRECTIVE_CSV_PARSE, DIRECTIVE_LINES, DIRECTIVE_LOG, DIRECTIVE_READ,
    DIRECTIVE_WATCH, DIRECTIVE_WRITE,
};
use crate::runtime::env::Value;
use csv::{ReaderBuilder, Trim};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// A builtin directive handler.
/// Takes a list of argument values and the current pipe value,
/// and returns a result value.
pub type DirectiveHandler = Arc<dyn Fn(Vec<Value>, Value) -> Result<Value, String> + Send + Sync>;

/// A builtin function handler.  
/// Takes a list of argument values and returns a result value.
pub type FunctionHandler = Arc<dyn Fn(Vec<Value>) -> Result<Value, String> + Send + Sync>;

pub struct BuiltinRegistry {
    directives: HashMap<String, DirectiveHandler>,
    functions: HashMap<String, FunctionHandler>,
}

impl Default for BuiltinRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl BuiltinRegistry {
    pub fn new() -> Self {
        let mut reg = Self {
            directives: HashMap::new(),
            functions: HashMap::new(),
        };
        reg.register_defaults();
        reg
    }

    pub fn get_directive(&self, name: &str) -> Option<DirectiveHandler> {
        self.directives.get(name).cloned()
    }

    pub fn get_builtin_function(&self, name: &str) -> Option<FunctionHandler> {
        self.functions.get(name).cloned()
    }

    pub fn directive_names(&self) -> Vec<&str> {
        self.directives.keys().map(|s| s.as_str()).collect()
    }

    pub fn function_names(&self) -> Vec<&str> {
        self.functions.keys().map(|s| s.as_str()).collect()
    }

    pub(crate) fn register_defaults(&mut self) {
        // @watch directive — watches a directory/file path, returns an event record
        self.directives.insert(
            DIRECTIVE_WATCH.to_string(),
            Arc::new(|args, _pipe_val| {
                let path = args
                    .first()
                    .map(|v| v.as_string())
                    .unwrap_or_else(|| ".".to_string());
                let mut event = HashMap::new();
                event.insert("file".to_string(), Value::Path(path.clone()));
                event.insert("path".to_string(), Value::Path(path));
                event.insert("type".to_string(), Value::String("created".to_string()));
                Ok(Value::Record(event))
            }),
        );

        // @atomic directive — marks a transaction boundary, passes through value
        self.directives.insert(
            DIRECTIVE_ATOMIC.to_string(),
            Arc::new(|_args, pipe_val| Ok(pipe_val)),
        );

        // @lines directive — reads a file line-by-line into a list
        self.directives.insert(
            DIRECTIVE_LINES.to_string(),
            Arc::new(|args, pipe_val| {
                let source = args.first().cloned().unwrap_or(pipe_val);
                let source_path = source
                    .as_path()
                    .ok_or_else(|| "@lines expects a file path source".to_string())?
                    .to_string();
                ensure_file_size_limit(&source_path)?;
                let text = std::fs::read_to_string(&source_path)
                    .map_err(|e| format!("Failed to read '{}': {}", source_path, e))?;
                let lines = text
                    .lines()
                    .map(|line| Value::String(line.to_string()))
                    .collect();
                Ok(Value::List(lines))
            }),
        );

        // @csv.parse directive — parses CSV data into rows keyed by header names
        self.directives.insert(
            DIRECTIVE_CSV_PARSE.to_string(),
            Arc::new(|_args, pipe_val| {
                let (source, csv_text) = match &pipe_val {
                    Value::Path(path) => {
                        ensure_file_size_limit(path)?;
                        let text = std::fs::read_to_string(path)
                            .map_err(|e| format!("Failed to read '{}': {}", path, e))?;
                        (path.clone(), text)
                    }
                    Value::String(text) => {
                        if text.contains('\n') || text.contains('\r') {
                            (text.clone(), text.clone())
                        } else if Path::new(text).exists() {
                            ensure_file_size_limit(text)?;
                            let file_text = std::fs::read_to_string(text)
                                .map_err(|e| format!("Failed to read '{}': {}", text, e))?;
                            (text.clone(), file_text)
                        } else {
                            (text.clone(), text.clone())
                        }
                    }
                    Value::List(items) => {
                        let text = items
                            .iter()
                            .map(|v| v.as_string())
                            .collect::<Vec<_>>()
                            .join("");
                        ("list".to_string(), text)
                    }
                    Value::Record(_) => {
                        // Records (e.g. watch events) may contain a file path — extract and read it
                        if let Some(path) = pipe_val.as_path() {
                            ensure_file_size_limit(path)?;
                            let file_text = std::fs::read_to_string(path)
                                .map_err(|e| format!("Failed to read '{}': {}", path, e))?;
                            (path.to_string(), file_text)
                        } else {
                            return Err(
                                "@csv.parse received a record without a file path".to_string()
                            );
                        }
                    }
                    other => {
                        let text = other.as_string();
                        if Path::new(&text).exists() {
                            ensure_file_size_limit(&text)?;
                            let file_text = std::fs::read_to_string(&text)
                                .map_err(|e| format!("Failed to read '{}': {}", text, e))?;
                            (text.clone(), file_text)
                        } else {
                            (text.clone(), text.clone())
                        }
                    }
                };

                let normalized = normalize_csv_for_parsing(&csv_text);
                let mut reader = ReaderBuilder::new()
                    .trim(Trim::All)
                    .from_reader(normalized.as_bytes());

                let headers = reader
                    .headers()
                    .map_err(|e| format!("Failed to parse CSV headers from '{}': {}", source, e))?
                    .iter()
                    .map(|h| h.to_string())
                    .collect::<Vec<_>>();

                let mut rows = Vec::new();
                for (row_index, row) in reader.records().enumerate() {
                    if row_index + 1 > max_rows_limit() {
                        return Err(format!(
                            "CSV row limit exceeded: {} > {}",
                            row_index + 1,
                            max_rows_limit()
                        ));
                    }
                    let record = row
                        .map_err(|e| format!("Failed to parse CSV row from '{}': {}", source, e))?;
                    if record.len() != headers.len() {
                        return Err(format!(
                            "CSV row {} has {} fields, but header has {} fields",
                            row_index + 2,
                            record.len(),
                            headers.len()
                        ));
                    }
                    let mut mapped = HashMap::new();
                    for (idx, value) in record.iter().enumerate() {
                        let key = headers
                            .get(idx)
                            .cloned()
                            .unwrap_or_else(|| format!("col{}", idx + 1));
                        mapped.insert(key, Value::String(value.to_string()));
                    }
                    rows.push(Value::Record(mapped));
                }

                let mut record = HashMap::new();
                record.insert("source".to_string(), Value::String(source));
                record.insert("valid".to_string(), Value::Boolean(true));
                record.insert(
                    "headers".to_string(),
                    Value::List(headers.iter().cloned().map(Value::String).collect()),
                );

                record.insert("rows".to_string(), Value::List(rows));
                Ok(Value::Record(record))
            }),
        );

        // @log directive — logs a value
        self.directives.insert(
            DIRECTIVE_LOG.to_string(),
            Arc::new(|_args, pipe_val| {
                println!("{}", pipe_val.as_string());
                Ok(pipe_val)
            }),
        );

        // @read directive — reads a file from arg or piped value
        self.directives.insert(
            DIRECTIVE_READ.to_string(),
            Arc::new(|args, pipe_val| {
                let source = args.first().cloned().unwrap_or(pipe_val);
                let path = extract_read_path(&source).ok_or_else(|| {
                    "@read expects a path, event, or variable containing a path".to_string()
                })?;
                ensure_file_size_limit(&path)?;
                match std::fs::read_to_string(&path) {
                    Ok(contents) => Ok(Value::String(contents)),
                    Err(e) => Err(format!("Failed to read '{}': {}", path, e)),
                }
            }),
        );

        // @write directive — writes to a file
        self.directives.insert(
            DIRECTIVE_WRITE.to_string(),
            Arc::new(|args, pipe_val| {
                let path = args
                    .first()
                    .map(|v| v.as_string())
                    .unwrap_or_else(|| "output.txt".to_string());
                match std::fs::write(&path, pipe_val.as_string()) {
                    Ok(()) => Ok(Value::String(path)),
                    Err(e) => Err(format!("Failed to write '{}': {}", path, e)),
                }
            }),
        );

        // filter function — filters items with a predicate
        self.functions.insert(
            "filter".to_string(),
            Arc::new(|args| Ok(args.into_iter().next().unwrap_or(Value::Null))),
        );

        // map function — maps items with a transform
        self.functions.insert(
            "map".to_string(),
            Arc::new(|args| Ok(args.into_iter().next().unwrap_or(Value::Null))),
        );

        // print function — prints a value to stdout
        self.functions.insert(
            "print".to_string(),
            Arc::new(|args| {
                let msg = args
                    .first()
                    .map(|v| v.as_string())
                    .unwrap_or_else(|| "".to_string());
                println!("{}", msg);
                Ok(Value::String(msg))
            }),
        );

        // concat function — concatenates values
        self.functions.insert(
            "concat".to_string(),
            Arc::new(|args| {
                let result: String = args
                    .iter()
                    .map(|v| v.as_string())
                    .collect::<Vec<_>>()
                    .join("");
                Ok(Value::String(result))
            }),
        );

        // exists function — checks if a file exists
        self.functions.insert(
            "exists".to_string(),
            Arc::new(|args| {
                let path = args
                    .first()
                    .map(|v| v.as_string())
                    .unwrap_or_else(|| "".to_string());
                Ok(Value::Boolean(Path::new(&path).exists()))
            }),
        );
    }
}

pub(crate) fn extract_read_path(value: &Value) -> Option<String> {
    match value {
        Value::Path(p) => Some(p.clone()),
        Value::String(s) => Some(s.clone()),
        Value::Record(map) => {
            if let Some(path) = map.get("path").and_then(extract_read_path) {
                return Some(path);
            }
            map.get("file").and_then(extract_read_path)
        }
        _ => None,
    }
}

pub(crate) fn normalize_csv_for_parsing(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_quotes = false;
    let mut at_field_start = true;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '"' {
            if in_quotes {
                if matches!(chars.peek(), Some('"')) {
                    out.push('"');
                    out.push('"');
                    let _ = chars.next();
                    at_field_start = false;
                    continue;
                }
                in_quotes = false;
                out.push(ch);
                at_field_start = false;
                continue;
            }
            in_quotes = true;
            out.push(ch);
            at_field_start = false;
            continue;
        }

        if !in_quotes && at_field_start && (ch == ' ' || ch == '\t') {
            continue;
        }

        out.push(ch);
        at_field_start = !in_quotes && (ch == '\n' || ch == '\r' || ch == ',');
    }

    out
}

fn max_file_size_limit() -> usize {
    std::env::var("LOOM_MAX_FILE_SIZE_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(32 * 1024 * 1024)
}

fn max_rows_limit() -> usize {
    std::env::var("LOOM_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(100_000)
}

fn ensure_file_size_limit(path: &str) -> Result<(), String> {
    let meta = std::fs::metadata(path).map_err(|e| format!("Failed to stat '{}': {}", path, e))?;
    if meta.len() as usize > max_file_size_limit() {
        return Err(format!(
            "File '{}' is {} bytes, above max_file_size_bytes ({})",
            path,
            meta.len(),
            max_file_size_limit()
        ));
    }
    Ok(())
}
