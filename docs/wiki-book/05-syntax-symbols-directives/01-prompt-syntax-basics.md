# Language Syntax Reference

This page documents Loom syntax based on the parser grammar and runtime behavior.

## 1) Program structure

A Loom program is a list of statements:
- `@import ...`
- function definitions
- pipe flows

Syntax forms:

```loom
@import "std.csv" as csv

is_valid(row) => row.id != null && row.price > 0

@read(\"./inbox/orders.csv") >> @csv.parse >> filter(row >> is_valid(row)) >> "./out/clean.csv"
```

## 2) Statements

### Import statement

```loom
@import "std.csv" as csv
@import "lib/logic"
```

Rules:
- Path is required.
- `as alias` is optional.
- Imports are top-level statements.

### Function definition

```loom
normalize(row) => row
sum(a, b, c) => a + b + c
```

Rules:
- Form: `name(param1, param2, ...) => <flow-or-branch>`.
- Function body can be a normal flow or a branch (`[...]`).

### Pipe flow

```loom
source >> destination >> destination on_fail as err >> recovery_flow
```

Rules:
- Flow starts with a source.
- Then zero or more piped destinations.
- Optional `on_fail` handler at the end.

## 3) Sources and destinations

A flow source can be:
- directive flow (`@read(...)`, `@watch(...)`)
- function call (`parse_csv(...)`)
- non-lambda expression

A destination can be:
- branch (`[...]`)
- directive flow
- function call
- non-lambda expression (including file path literal targets)

## 4) Operators

Pipe operators:
- `>>` safe pipe
: For file path destinations, appends content.
- `>>>` force pipe
: For file path destinations, overwrites content.
- `->` move pipe
: Moves file from left path to right path.

Examples:

```loom
\"line" >> "./logs/app.log"
\"latest" >>> "./out/state.json"
"./inbox/new.csv" -> "./archive/new.csv"
```

## 4.1) Critical difference from most languages: quotes mean files by default

Loom treats plain quoted literals (`"..."`) as path literals, not normal string literals.

That means:
- In argument/parameter positions, `"..."` is treated as a file path to read immediately, and the file contents are passed as the argument value.
- In destination positions, `"..."` is treated as a file output target.
- If you want literal text, you must use escaped string literals (`\"..."`) or template literals.

Examples:

```loom
@secret("API_KEY_FILE")           // reads file API_KEY_FILE; file contents become key name
@secret(\"API_KEY\")              // literal key text API_KEY
@http.post(url: \"https://api\")  // literal URL text
```

This is intentionally different from Python/JavaScript/etc, where `"..."` is usually a normal string.

Path-aware pipeline behavior:
- Source path literal is treated as a file path input value (consumed by directives/sinks such as `@read`, `@csv.parse`, or file destinations).
- Destination path literal acts as file output target.

Examples:

```loom
"./inbox/orders.csv" >> @read >> stdout   // read a file via path literal source
\"processed" >> "./logs/activity.log"     // append literal text to file
\"state" >>> "./out/state.txt"            // overwrite file with literal text
```

Use escaped string literals (`\"..."`) for text data, not paths:

```loom
\"hello world" >> stdout
```

Use template literals for interpolated text:

```loom
`processed ${event.file.name}` >> "./logs/activity.log"
```

## 5) Branch syntax

Branch fan-out:

```loom
source >> [
  flow_a,
  flow_b,
  flow_c
]
```

Notes:
- Each branch item is a full flow.
- Branch items are comma-separated.
- Empty branches are valid (`[]`) but rarely useful.

## 6) Error handling (`on_fail`)

Forms:

```loom
flow on_fail >> "./logs/errors.log"
flow on_fail as err >> [err >> "./logs/errors.log"]
flow on_fail as err >> err >> "./logs/errors.log"
```

Rules:
- `on_fail` can have alias (`as err`) or no alias.
- Handler can be a flow or a branch.

Important behavior:
- Runtime errors trigger `on_fail`.
- A filter-rejection error (`Filter condition failed`) is treated specially and does not run `on_fail`.

## 7) Expressions

Expression forms:
- identifiers: `event`, `row`, `util`
- literals: string/path/template/number/boolean
- member access: `event.file.path`
- function calls: `exists(\"./x")`, `util.is_valid(row)`
- secret expressions: `@secret(\"NAME\")`
- unary not: `!event.active`
- binary operators: `+ - * / > >= < <= == != && ||`
- lambda: `row >> row.price > 1000`

Binary operator precedence (high to low):
1. `*` `/`
2. `+` `-`
3. `>` `<` `>=` `<=`
4. `==` `!=`
5. `&&`
6. `||`

## 8) Literals and strings

### Path literal

```loom
"./inbox/orders.csv"
```

This form is used heavily for file sources and destinations.

### String literal

Escaped-string form:

```loom
\"hello"
```

Use this when you want string data and do not want path semantics.

You can compose strings with `@secret(...)` anywhere expressions are allowed:

```loom
\"hello: " + @secret(\"NAME")
```

### Template string

```loom
`order ${row.id} -> ${row.price}`
```

Template interpolation rules:
- `${ ... }` supports normal expressions.
- Escape sequences include ``\` ``, `\$`, and `\\`.

### Number and boolean literals

```loom
42
-42.5
true
false
```

## 9) Lambda syntax

Form:

```loom
param >> expression
```

Examples:

```loom
filter(row >> row.price > 1000)
map(row >> row.email)
```

## 10) Aliasing with `as`

`as` binds intermediate values:

```loom
@csv.parse as data
@watch(\"./inbox") as event
on_fail as err >> err >> "./logs/error.log"
```

## 11) Built-in functions and common calls

Function call form:

```loom
name(arg1, arg2)
```

## 12) Named arguments and object literals

Calls support positional and named arguments:

```loom
@http.post(url: \"http://127.0.0.1:8123/api", headers: { "Authorization": \"Bearer token" })
```

Object literals use `{ key: value }` with identifier or string keys.

## 13) HTTP standard library (`std.http`)

Import and use:

```loom
@import "std.http" as http

42 >> @http.post(\"http://127.0.0.1:8123/api") >> "response.txt"
```

Behavior:
- `@http.post(url, headers?, data?)`
- If `data` is omitted, piped input becomes request body.
- Body encoding is string-based (`as_string()` semantics).
- The response body string is piped to the next step.
- Non-2xx status codes raise runtime errors (usable with `on_fail`).

Built-in functions available in runtime include:
- `filter(...)`
- `map(...)`
- `print(...)`
- `concat(...)`
- `exists(...)`

## 14) Comments and whitespace

Single-line comments:

```loom
// this is a comment
```

Whitespace and comments are ignored by parsing around tokens.
