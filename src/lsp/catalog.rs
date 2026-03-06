use crate::builtin_spec::{
    BUILTIN_DIRECTIVES, BUILTIN_FUNCTIONS, BuiltinDirectiveSpec, BuiltinFunctionSpec,
};

pub(crate) type DirectiveInfo = BuiltinDirectiveSpec;
pub(crate) type BuiltinFunctionInfo = BuiltinFunctionSpec;

/// Built-in directive documentation for hover and completion.
pub(crate) const DIRECTIVES: &[DirectiveInfo] = BUILTIN_DIRECTIVES;
pub(crate) const BUILTIN_FUNCTION_DOCS: &[BuiltinFunctionInfo] = BUILTIN_FUNCTIONS;

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

pub(crate) const MEMBER_FIELDS: &[(&str, &str)] = &[
    ("file", "The file path from a watch event"),
    ("path", "The full path of the resource"),
    ("type", "The type of event (created, modified, deleted)"),
    ("valid", "Whether the record passed validation"),
    ("data", "The data content"),
    ("size", "The size of the file"),
    ("source", "The source of the data"),
    ("rows", "Parsed rows from CSV data"),
    ("length", "Length of a string value"),
    ("name", "Name of the resource"),
];
