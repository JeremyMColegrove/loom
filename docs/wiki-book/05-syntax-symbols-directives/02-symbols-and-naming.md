# Control Flow Without If Statements

Loom does not have a standalone `if / else` statement in grammar.

Use dataflow primitives instead:
- `filter(...)` for conditional gating
- branch fan-out (`[...]`) for parallel routing
- `on_fail` for error path control

## 1) `if` equivalent: gate a flow

```loom
@read("./inbox/orders.csv") >> @csv.parse >> filter(row >> row.amount > 1000) >> "./out/high-value.csv"
```

Interpretation:
- Keep rows where predicate is true.
- Drop rows where predicate is false.

## 2) `if / else` equivalent: split into two branches

```loom
@read("./inbox/orders.csv") >> @csv.parse >> [
  filter(row >> row.amount > 1000) >> "./out/high.csv",
  filter(row >> row.amount <= 1000) >> "./out/normal.csv"
]
```

Interpretation:
- First branch behaves like `if` path.
- Second branch behaves like `else` path.

## 3) Guard the whole pipeline

```loom
@watch("./inbox") as event >> filter(event.type == "created" && event.file.ext == "csv") >> @read(event.file.path)
```

This is the normal replacement for:
- `if event.type == ... then process`

## 4) Use predicate helpers for readability

```loom
is_target(event) => event.type == "created" && event.file.ext == "csv"

@watch("./inbox") as event >> filter(is_target(event)) >> @read(event.file.path)
```

This keeps flow topology clear while centralizing condition logic.

## 5) `on_fail` is not `else`

`on_fail` is only for runtime errors.

```loom
@read("./inbox/orders.csv") >> @csv.parse on_fail as err >> err >> "./logs/error.log"
```

Do not use it as a normal false-condition branch.

## 6) Important edge case: filter rejection behavior

When `filter` is used as a boolean gate and condition is false, runtime returns `Filter condition failed`.

That failure does not execute `on_fail`.

Design implication:
- Use normal branch filters for data-routing logic.
- Reserve `on_fail` for genuine runtime failures (missing functions, I/O failures, parse failures, policy denials, etc.).

## 7) Flow organization rules

For maintainable control flow:
- Keep one ingestion source per top-level script when possible.
- Keep conditions near the data they guard.
- Prefer named helper functions over repeated inline predicates.
- Use branch blocks for fan-out, not nested monolithic expressions.
