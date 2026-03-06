# Directive and Event Reference

This page is the runtime directive reference.

## Runtime directives

Known runtime directives:
- `@watch(path, recursive?, debounce_ms?)`
- `@atomic`
- `@lines(path?)`
- `@csv.parse(data)`
- `@log`
- `@read(path)`
- `@write(path)`
- `@filter(predicate)`
- `@map(transform)`

## Path literals vs `@read(...)`

Use plain path literals (`"..."`) when you already have a static file path in code.

Example:

```loom
"./inbox/my_file.txt" >> stdout
```

Use `@read(...)` when the path comes from a variable/record at runtime (for example a watch event).

Example:

```loom
@watch("./inbox") as event >> @read(event.file.path) >> stdout
```

Use escaped string literals (`\"..."`) or template literals (`` `...` ``) when you want text values instead of path values.

## 1) `@watch(path, recursive?, debounce_ms?)`

Purpose:
- Watches files/directories and emits event records.

Accepted arguments:
- First arg: path (required shape: path-like value).
- Optional boolean arg: recursive mode.
- Optional numeric arg: debounce milliseconds.
- Optional record arg with keys:
  - `recursive` (boolean)
  - `debounce_ms` (number)
  - `debounce` (number alias)

Defaults and normalization:
- Default path: `"."` when missing.
- Default `recursive`: `false`.
- Default debounce: `200ms`.
- Numeric debounce values are clamped to minimum `10ms`.

Event alias pattern:

```loom
@watch("./inbox") as event >> filter(event.type == "created") >> @read(event.file.path)
```

Event record shape (runtime):
- `event.type`: string event type (`created`, `modified`, `deleted`)
- `event.path`: string full path
- `event.file`: record with:
  - `event.file.path`: string path
  - `event.file.name`: filename
  - `event.file.ext`: extension
  - `event.file.created_at.year`: approximate year from file timestamp

Event-type notes:
- Rename notifications are normalized into `created`/`deleted` forms depending on file existence.
- Burst/coalescing behavior prefers preserving `created` during debounce windows.

## 2) `@read(path)`

Purpose:
- Reads file content as string.

Accepted source values:
- Explicit path argument.
- Piped path/string.
- Piped record containing `path` or `file` path fields.

Typical use:
- Dynamic runtime path extraction (`event.file.path`, variable-held paths).
- Reading via event records produced by `@watch`.

Failure cases:
- Missing/invalid path extraction.
- Unauthorized path per policy.
- File stat/read failures.

## 3) `@write(path)`

Purpose:
- Writes current piped value to a target file.

Behavior:
- Uses first argument as path.
- Defaults to `output.txt` if no path arg is supplied.
- Returns written path value.

Notes:
- Prefer pipe operators into path literals for explicit append vs overwrite semantics (`>>` vs `>>>`).

## 4) `@lines(path?)`

Purpose:
- Reads a file into `List<String>` (line-by-line).

Behavior:
- Uses explicit argument when provided.
- Otherwise uses piped value as source.

## 5) `@csv.parse(data)`

Purpose:
- Parses CSV and returns a structured record.

Accepted input modes:
- Piped path.
- Piped CSV string.
- Piped list (joined into a CSV string).
- Piped record containing path.
- String that points to existing file path.

Return shape:
- `source`: string source identifier
- `valid`: boolean
- `headers`: list of header names
- `rows`: list of row records

Row records:
- Each row is `Record<String, String>` keyed by CSV header.

Limits:
- `LOOM_MAX_FILE_SIZE_BYTES` (default `32MB`)
- `LOOM_MAX_ROWS` (default `100000`)

Failure cases:
- Header parse failures.
- Row parse failures.
- Header/row field count mismatch.
- Source without usable file/text.

## 6) `@atomic`

Purpose:
- Transaction boundary for file mutations.

Behavior:
- Wraps subsequent file operations in rollback-aware scope.
- If a later step fails, file writes/moves performed in that scope are rolled back.

Typical use:

```loom
@watch("./inbox") as event >> @atomic >> [
  @read(event.file.path) >> @csv.parse >> "./out/clean.csv",
  event.file -> "./archive/"
] on_fail as err >> event.file -> "./quarantine/"
```

## 7) `@log`

Purpose:
- Debug inspection.

Behavior:
- Prints current value.
- Passes value through unchanged.

## 8) `@filter(...)` and `@map(...)` directive forms

These are directive forms of built-in functions.

Examples:

```loom
@read("./inbox/orders.csv") >> @csv.parse >> @filter(row >> row.amount > 1000)
@read("./inbox/orders.csv") >> @csv.parse >> @map(row >> row.email)
```

## 9) `@import` note

`@import` is a top-level statement (module loading), not a runtime pipe directive.

Syntax:

```loom
@import "std.csv" as csv
```
