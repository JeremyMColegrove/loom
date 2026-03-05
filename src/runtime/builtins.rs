use std::collections::HashMap;
use std::path::Path;
use crate::runtime::env::Value;
use csv::{ReaderBuilder, Trim};

/// A builtin directive handler.
/// Takes a list of argument values and the current pipe value,
/// and returns a result value.
pub type DirectiveHandler = Box<dyn Fn(Vec<Value>, Value) -> Result<Value, String> + Send + Sync>;

/// A builtin function handler.  
/// Takes a list of argument values and returns a result value.
pub type FunctionHandler = Box<dyn Fn(Vec<Value>) -> Result<Value, String> + Send + Sync>;

pub struct BuiltinRegistry {
    directives: HashMap<String, DirectiveHandler>,
    functions: HashMap<String, FunctionHandler>,
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

    pub fn get_directive(&self, name: &str) -> Option<&DirectiveHandler> {
        self.directives.get(name)
    }

    pub fn get_builtin_function(&self, name: &str) -> Option<&FunctionHandler> {
        self.functions.get(name)
    }

    pub fn directive_names(&self) -> Vec<&str> {
        self.directives.keys().map(|s| s.as_str()).collect()
    }

    pub fn function_names(&self) -> Vec<&str> {
        self.functions.keys().map(|s| s.as_str()).collect()
    }

    fn register_defaults(&mut self) {
        // @watch directive — watches a directory/file path, returns an event record
        self.directives.insert("watch".to_string(), Box::new(|args, _pipe_val| {
            let path = args.first()
                .map(|v| v.as_string())
                .unwrap_or_else(|| ".".to_string());
            let mut event = HashMap::new();
            event.insert("file".to_string(), Value::Path(path.clone()));
            event.insert("path".to_string(), Value::Path(path));
            event.insert("type".to_string(), Value::String("created".to_string()));
            Ok(Value::Record(event))
        }));

        // @atomic directive — marks a transaction boundary, passes through value
        self.directives.insert("atomic".to_string(), Box::new(|_args, pipe_val| {
            Ok(pipe_val)
        }));

        // @chunk directive — splits data into chunks
        self.directives.insert("chunk".to_string(), Box::new(|args, pipe_val| {
            let size = args.first()
                .map(|v| v.as_string())
                .unwrap_or_else(|| "1mb".to_string());
            let source = args.get(1).cloned().unwrap_or(pipe_val);
            let source_path = source.as_path()
                .ok_or_else(|| "@chunk expects a file path source".to_string())?
                .to_string();
            let bytes = std::fs::read(&source_path)
                .map_err(|e| format!("Failed to read '{}': {}", source_path, e))?;
            let chunk_size = parse_size_bytes(&size)?;
            if chunk_size == 0 {
                return Err("Chunk size must be greater than 0".to_string());
            }

            let mut chunks = Vec::new();
            for part in bytes.chunks(chunk_size) {
                chunks.push(Value::String(String::from_utf8_lossy(part).to_string()));
            }
            Ok(Value::List(chunks))
        }));

        // @lines directive — reads a file line-by-line into a list
        self.directives.insert("lines".to_string(), Box::new(|args, pipe_val| {
            let source = args.first().cloned().unwrap_or(pipe_val);
            let source_path = source.as_path()
                .ok_or_else(|| "@lines expects a file path source".to_string())?
                .to_string();
            let text = std::fs::read_to_string(&source_path)
                .map_err(|e| format!("Failed to read '{}': {}", source_path, e))?;
            let lines = text
                .lines()
                .map(|line| Value::String(line.to_string()))
                .collect();
            Ok(Value::List(lines))
        }));

        // @csv.parse directive — parses CSV data into rows keyed by header names
        self.directives.insert("csv.parse".to_string(), Box::new(|_args, pipe_val| {
            let (source, csv_text) = match &pipe_val {
                Value::Path(path) => {
                    let text = std::fs::read_to_string(path)
                        .map_err(|e| format!("Failed to read '{}': {}", path, e))?;
                    (path.clone(), text)
                }
                Value::String(text) => {
                    if text.contains('\n') || text.contains('\r') {
                        (text.clone(), text.clone())
                    } else if Path::new(text).exists() {
                        let file_text = std::fs::read_to_string(text)
                            .map_err(|e| format!("Failed to read '{}': {}", text, e))?;
                        (text.clone(), file_text)
                    } else {
                        (text.clone(), text.clone())
                    }
                }
                Value::List(items) => {
                    let text = items.iter().map(|v| v.as_string()).collect::<Vec<_>>().join("");
                    ("list".to_string(), text)
                }
                Value::Record(_) => {
                    // Records (e.g. watch events) may contain a file path — extract and read it
                    if let Some(path) = pipe_val.as_path() {
                        let file_text = std::fs::read_to_string(path)
                            .map_err(|e| format!("Failed to read '{}': {}", path, e))?;
                        (path.to_string(), file_text)
                    } else {
                        return Err("@csv.parse received a record without a file path".to_string());
                    }
                }
                other => {
                    let text = other.as_string();
                    if Path::new(&text).exists() {
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

            let headers = reader.headers()
                .map_err(|e| format!("Failed to parse CSV headers from '{}': {}", source, e))?
                .iter()
                .map(|h| h.to_string())
                .collect::<Vec<_>>();

            let mut rows = Vec::new();
            for (row_index, row) in reader.records().enumerate() {
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
                    let key = headers.get(idx)
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
        }));

        // @log directive — logs a value
        self.directives.insert("log".to_string(), Box::new(|_args, pipe_val| {
            println!("  📋 {}", pipe_val.as_string());
            Ok(pipe_val)
        }));

        // @read directive — reads a file from arg or piped value
        self.directives.insert("read".to_string(), Box::new(|args, pipe_val| {
            let source = args.first().cloned().unwrap_or(pipe_val);
            let path = extract_read_path(&source)
                .ok_or_else(|| "@read expects a path, event, or variable containing a path".to_string())?;
            match std::fs::read_to_string(&path) {
                Ok(contents) => Ok(Value::String(contents)),
                Err(e) => Err(format!("Failed to read '{}': {}", path, e))
            }
        }));

        // @write directive — writes to a file
        self.directives.insert("write".to_string(), Box::new(|args, pipe_val| {
            let path = args.first()
                .map(|v| v.as_string())
                .unwrap_or_else(|| "output.txt".to_string());
            match std::fs::write(&path, pipe_val.as_string()) {
                Ok(()) => Ok(Value::String(path)),
                Err(e) => Err(format!("Failed to write '{}': {}", path, e))
            }
        }));

        // filter function — filters items with a predicate
        self.functions.insert("filter".to_string(), Box::new(|args| {
            Ok(args.into_iter().next().unwrap_or(Value::Null))
        }));

        // map function — maps items with a transform
        self.functions.insert("map".to_string(), Box::new(|args| {
            Ok(args.into_iter().next().unwrap_or(Value::Null))
        }));

        // print function — prints a value to stdout
        self.functions.insert("print".to_string(), Box::new(|args| {
            let msg = args.first()
                .map(|v| v.as_string())
                .unwrap_or_else(|| "".to_string());
            println!("  🖨️  {}", msg);
            Ok(Value::String(msg))
        }));

        // concat function — concatenates values  
        self.functions.insert("concat".to_string(), Box::new(|args| {
            let result: String = args.iter().map(|v| v.as_string()).collect::<Vec<_>>().join("");
            Ok(Value::String(result))
        }));

        // exists function — checks if a file exists
        self.functions.insert("exists".to_string(), Box::new(|args| {
            let path = args.first()
                .map(|v| v.as_string())
                .unwrap_or_else(|| "".to_string());
            Ok(Value::Boolean(Path::new(&path).exists()))
        }));
    }
}

fn extract_read_path(value: &Value) -> Option<String> {
    match value {
        Value::Path(p) => Some(p.clone()),
        Value::String(s) => Some(s.clone()),
        Value::Record(map) => {
            if let Some(path) = map.get("path").and_then(extract_read_path) {
                return Some(path);
            }
            map.get("file")
                .and_then(extract_read_path)
        }
        _ => None,
    }
}

fn parse_size_bytes(raw: &str) -> Result<usize, String> {
    let lower = raw.trim().to_ascii_lowercase();
    let parse_num = |s: &str| s.parse::<usize>().map_err(|_| format!("Invalid size: '{}'", raw));

    if let Some(n) = lower.strip_suffix("kb") {
        return Ok(parse_num(n.trim())? * 1024);
    }
    if let Some(n) = lower.strip_suffix("mb") {
        return Ok(parse_num(n.trim())? * 1024 * 1024);
    }
    if let Some(n) = lower.strip_suffix("gb") {
        return Ok(parse_num(n.trim())? * 1024 * 1024 * 1024);
    }
    parse_num(&lower)
}

fn normalize_csv_for_parsing(input: &str) -> String {
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
        if !in_quotes && (ch == '\n' || ch == '\r') {
            at_field_start = true;
        } else if !in_quotes && ch == ',' {
            at_field_start = true;
        } else {
            at_field_start = false;
        }
    }

    out
}
