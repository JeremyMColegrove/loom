/// Shared metadata for built-in directives understood by the runtime.
#[derive(Clone, Copy, Debug)]
pub struct BuiltinDirectiveSpec {
    pub name: &'static str,
    pub signature: &'static str,
    pub description: &'static str,
}

pub const DIRECTIVE_WATCH: &str = "watch";
pub const DIRECTIVE_ATOMIC: &str = "atomic";
pub const DIRECTIVE_CHUNK: &str = "chunk";
pub const DIRECTIVE_LINES: &str = "lines";
pub const DIRECTIVE_CSV_PARSE: &str = "csv.parse";
pub const DIRECTIVE_LOG: &str = "log";
pub const DIRECTIVE_READ: &str = "read";
pub const DIRECTIVE_WRITE: &str = "write";
pub const DIRECTIVE_FILTER: &str = "filter";
pub const DIRECTIVE_MAP: &str = "map";

pub const BUILTIN_DIRECTIVES: &[BuiltinDirectiveSpec] = &[
    BuiltinDirectiveSpec {
        name: DIRECTIVE_WATCH,
        signature: "@watch(path, recursive?, debounce_ms?)",
        description: "Watches a file or directory using filesystem events. Optional args enable recursive mode and debounce in milliseconds. Returns an event record with `file`, `path`, and `type` fields.",
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_ATOMIC,
        signature: "@atomic",
        description: "Wraps subsequent operations in a transaction. If any step fails, all changes are rolled back.",
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_CHUNK,
        signature: "@chunk(size, source)",
        description: "Splits the input into chunks of the given size (e.g. `\"5mb\"`). Returns chunk records.",
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_LINES,
        signature: "@lines(path?)",
        description: "Reads a file line-by-line into a list of strings. Uses piped input when no argument is provided.",
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_CSV_PARSE,
        signature: "@csv.parse(data)",
        description: "Parses CSV data into records. Returns a record with `source`, `valid`, `headers`, and `rows` fields.",
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_LOG,
        signature: "@log",
        description: "Logs the current pipe value to stdout. Passes the value through unchanged.",
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_READ,
        signature: "@read(path)",
        description: "Reads the contents of a file and returns it as a string.",
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_WRITE,
        signature: "@write(path)",
        description: "Writes the current pipe value to a file at the given path.",
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_FILTER,
        signature: "@filter(predicate)",
        description: "Directive form of `filter(...)` for piped list values.",
    },
    BuiltinDirectiveSpec {
        name: DIRECTIVE_MAP,
        signature: "@map(transform)",
        description: "Directive form of `map(...)` for piped list values.",
    },
];

pub fn is_known_runtime_directive(name: &str) -> bool {
    BUILTIN_DIRECTIVES.iter().any(|dir| dir.name == name) || name.ends_with(".parse")
}
