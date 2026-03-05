/// Built-in directive documentation for hover and completion.
pub(crate) struct DirectiveInfo {
    pub(crate) name: &'static str,
    pub(crate) signature: &'static str,
    pub(crate) description: &'static str,
}

pub(crate) const DIRECTIVES: &[DirectiveInfo] = &[
    DirectiveInfo {
        name: "watch",
        signature: "@watch(path, recursive?, debounce_ms?)",
        description: "Watches a file or directory using filesystem events. Optional args enable recursive mode and debounce in milliseconds. Returns an event record with `file`, `path`, and `type` fields.",
    },
    DirectiveInfo {
        name: "atomic",
        signature: "@atomic",
        description: "Wraps subsequent operations in a transaction. If any step fails, all changes are rolled back.",
    },
    DirectiveInfo {
        name: "chunk",
        signature: "@chunk(size, source)",
        description: "Splits the input into chunks of the given size (e.g. `\"5mb\"`). Returns chunk records.",
    },
    DirectiveInfo {
        name: "csv.parse",
        signature: "@csv.parse(data)",
        description: "Parses CSV data into records. Returns a record with `source`, `valid`, and `rows` fields.",
    },
    DirectiveInfo {
        name: "log",
        signature: "@log",
        description: "Logs the current pipe value to stdout. Passes the value through unchanged.",
    },
    DirectiveInfo {
        name: "read",
        signature: "@read(path)",
        description: "Reads the contents of a file and returns it as a string.",
    },
    DirectiveInfo {
        name: "write",
        signature: "@write(path)",
        description: "Writes the current pipe value to a file at the given path.",
    },
    DirectiveInfo {
        name: "import",
        signature: "@import \"path\" [as alias]",
        description: "Imports functions and definitions from another Loom file.",
    },
];

pub(crate) const KEYWORDS: &[(&str, &str)] = &[
    (
        "on_fail",
        "Error handler block. Catches errors from the preceding pipe flow.",
    ),
    (
        "as",
        "Binds the result of a directive or on_fail to a named variable.",
    ),
    ("true", "Boolean literal true."),
    ("false", "Boolean literal false."),
];

pub(crate) const BUILTIN_FUNCTIONS: &[(&str, &str, &str)] = &[
    (
        "filter",
        "filter(predicate)",
        "Filters items using a lambda predicate. E.g. `filter(r >> r.valid)`",
    ),
    (
        "map",
        "map(transform)",
        "Transforms each item using a lambda. E.g. `map(r >> r.name)`",
    ),
    ("print", "print(value)", "Prints a value to stdout."),
    (
        "concat",
        "concat(a, b, ...)",
        "Concatenates values into a single string.",
    ),
    (
        "exists",
        "exists(path)",
        "Returns true if the file at path exists.",
    ),
];

pub(crate) const MEMBER_FIELDS: &[(&str, &str)] = &[
    ("file", "The file path from a watch event or chunk"),
    ("path", "The full path of the resource"),
    ("type", "The type of event (created, modified, deleted)"),
    ("valid", "Whether the record passed validation"),
    ("data", "The data content"),
    ("size", "The size of the chunk or file"),
    ("source", "The source of the data"),
    ("rows", "Parsed rows from CSV data"),
    ("length", "Length of a string value"),
    ("name", "Name of the resource"),
];
