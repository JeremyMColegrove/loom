# Core Parts Of A Loom Program

A Loom program has a small set of core building blocks. Learning these gives you a full map of the language.

## 1) Statements

A program is a list of statements. In practice, these are usually:
- Imports: `@import "std.csv" as csv`
- Function definitions: `is_valid(row) => ...`
- Pipeline flows: `source >> step >> step`

## 2) Flows and operations

Each flow has:
- A source value (`@watch(...)`, `@read(...)`, function call, or expression).
- Zero or more operations with pipe operators.
- Optional `on_fail` recovery behavior.

Operator roles:
- `>>` append pipe (for file destinations).
- `>>>` overwrite pipe (for file destinations).
- `->` move operation for file relocation.

## 3) Data model

Runtime values include strings, numbers, booleans, lists, records, paths, and null.

That means CSV parsing can return record/list structures, then `filter(...)` and `map(...)` can operate directly on rows.

## 4) Side-effect boundaries

Directives are explicit boundaries where external effects happen, such as `@read`, `@write`, `@watch`, and `@atomic`.

Because these boundaries are visible in syntax, it is easier to audit scripts and reason about risk.

## Example

```loom
@read(\"./inbox/customers.csv") >> @csv.parse as data >> filter(row >> row.email != \"") >> "./out/clean-customers.csv"
```

This single line already shows source, transformations, and destination clearly.
