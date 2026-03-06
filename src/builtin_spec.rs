/// Shared metadata for built-in language surface used by runtime, validator, and LSP.
#[derive(Clone, Copy, Debug)]
pub struct BuiltinDirectiveSpec {
    pub name: &'static str,
    pub signature: &'static str,
    pub description: &'static str,
    pub runtime_available: bool,
    pub required_std_module: Option<&'static str>,
}

#[derive(Clone, Copy, Debug)]
pub struct BuiltinFunctionSpec {
    pub name: &'static str,
    pub signature: &'static str,
    pub description: &'static str,
}

pub const DIRECTIVE_WATCH: &str = "watch";
pub const DIRECTIVE_ATOMIC: &str = "atomic";
pub const DIRECTIVE_LINES: &str = "lines";
pub const DIRECTIVE_CSV_PARSE: &str = "csv.parse";
pub const DIRECTIVE_LOG: &str = "log";
pub const DIRECTIVE_READ: &str = "read";
pub const DIRECTIVE_WRITE: &str = "write";
pub const DIRECTIVE_SECRET: &str = "secret";
pub const DIRECTIVE_FILTER: &str = "filter";
pub const DIRECTIVE_MAP: &str = "map";
pub const DIRECTIVE_HTTP_POST: &str = "http.post";
pub const DIRECTIVE_IMPORT: &str = "import";

pub const BUILTIN_DIRECTIVES: &[BuiltinDirectiveSpec] = &[
    BuiltinDirectiveSpec {
        name: DIRECTIVE_WATCH,
        signature: "@watch(path, recursive?, debounce_ms?)",
        description: "Watches a file or directory using filesystem events. Optional args enable recursive mode and debounce in milliseconds. Returns an event record with `file`, `path`, and `type` fields.",
        runtime_available: true,
        required_std_module: None,
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_ATOMIC,
        signature: "@atomic",
        description: "Wraps subsequent operations in a transaction. If any step fails, all changes are rolled back.",
        runtime_available: true,
        required_std_module: None,
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_LINES,
        signature: "@lines(path?)",
        description: "Reads a file line-by-line into a list of strings. Uses piped input when no argument is provided.",
        runtime_available: true,
        required_std_module: None,
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_CSV_PARSE,
        signature: "@csv.parse(data)",
        description: "Parses CSV data into records. Returns a record with `source`, `valid`, `headers`, and `rows` fields.",
        runtime_available: true,
        required_std_module: None,
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_LOG,
        signature: "@log",
        description: "Logs the current pipe value to stdout. Passes the value through unchanged.",
        runtime_available: true,
        required_std_module: None,
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_READ,
        signature: "@read(path)",
        description: "Reads the contents of a file and returns it as a string.",
        runtime_available: true,
        required_std_module: None,
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_WRITE,
        signature: "@write(path)",
        description: "Writes the current pipe value to a file at the given path.",
        runtime_available: true,
        required_std_module: None,
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_SECRET,
        signature: "@secret(key)",
        description: "Resolves a secret key from .env first, then process environment variables. Raises an error when the key is missing.",
        runtime_available: true,
        required_std_module: None,
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_FILTER,
        signature: "@filter(predicate)",
        description: "Directive form of `filter(...)` for piped list values.",
        runtime_available: true,
        required_std_module: None,
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_MAP,
        signature: "@map(transform)",
        description: "Directive form of `map(...)` for piped list values.",
        runtime_available: true,
        required_std_module: None,
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_HTTP_POST,
        signature: "@http.post(url, headers?, data?)",
        description: "Sends an HTTP POST request. Requires @import \"std.http\" as http. Uses piped input as request body when data is omitted.",
        runtime_available: false,
        required_std_module: Some("http"),
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_IMPORT,
        signature: "@import \"path\" [as alias]",
        description: "Imports functions and definitions from another Loom file.",
        runtime_available: false,
        required_std_module: None,
    },
];

pub const BUILTIN_FUNCTIONS: &[BuiltinFunctionSpec] = &[
    BuiltinFunctionSpec {
        name: "filter",
        signature: "filter(predicate)",
        description: "Filters items using a lambda predicate. E.g. `filter(r >> r.valid)`",
    },
    BuiltinFunctionSpec {
        name: "map",
        signature: "map(transform)",
        description: "Transforms each item using a lambda. E.g. `map(r >> r.name)`",
    },
    BuiltinFunctionSpec {
        name: "print",
        signature: "print(value)",
        description: "Prints a value to stdout.",
    },
    BuiltinFunctionSpec {
        name: "concat",
        signature: "concat(a, b, ...)",
        description: "Concatenates values into a single string.",
    },
    BuiltinFunctionSpec {
        name: "exists",
        signature: "exists(path)",
        description: "Returns true if the file at path exists.",
    },
];

pub fn is_known_runtime_directive(name: &str) -> bool {
    BUILTIN_DIRECTIVES
        .iter()
        .any(|dir| dir.runtime_available && dir.name == name)
}

pub fn required_std_module_for_directive(name: &str) -> Option<&'static str> {
    BUILTIN_DIRECTIVES
        .iter()
        .find(|dir| dir.name == name)
        .and_then(|dir| dir.required_std_module)
}

pub fn is_known_builtin_function(name: &str) -> bool {
    BUILTIN_FUNCTIONS.iter().any(|func| func.name == name)
}
