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

@read("./inbox/orders.csv") >> @csv.parse >> filter(row >> is_valid(row)) >> "./out/clean.csv"
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
"line" >> "./logs/app.log"
"latest" >>> "./out/state.json"
"./inbox/new.csv" -> "./archive/new.csv"
```

## 4.1) Implicit path I/O vs string literals

Loom treats plain quoted literals (`"..."`) as path literals.

That gives implicit filesystem behavior in path-aware pipelines:
- Source path literal is treated as a file path input value (consumed by path-aware directives/sinks such as `@read`, `@csv.parse`, or file destinations).
- Destination path literal acts as file output target.

Examples:

```loom
"./inbox/orders.csv" >> stdout            // read this file path into downstream step
"processed" >> "./logs/activity.log"      // append text to file
"state" >>> "./out/state.txt"             // overwrite file
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
- function calls: `exists("./x")`, `util.is_valid(row)`
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
@watch("./inbox") as event
on_fail as err >> err >> "./logs/error.log"
```

## 11) Built-in functions and common calls

Function call form:

```loom
name(arg1, arg2)
module.func(arg)
```

Built-in functions available in runtime include:
- `filter(...)`
- `map(...)`
- `print(...)`
- `concat(...)`
- `exists(...)`

## 12) Comments and whitespace

Single-line comments:

```loom
// this is a comment
```

Whitespace and comments are ignored by parsing around tokens.
