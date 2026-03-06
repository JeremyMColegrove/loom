# Your First Small Task

Your first Loom task should be narrow, observable, and reversible.

## Good first-task pattern

Pick a single input and a single output with one transform.

Example:

```loom
@read("./inbox/customers.csv") >> @csv.parse >> filter(row >> row.email != "") >> "./out/customers-with-email.csv"
```

Why this works well:
- Input is explicit.
- Rule is easy to understand.
- Output is easy to inspect.

## Avoid these first-task mistakes

- Combining watch + imports + branching + multiple outputs immediately.
- Running with broad permissions before behavior is known.
- Skipping output checks and trusting execution success alone.

## Add one change at a time

Once the first task works:
1. Add a second filter.
2. Add `on_fail` logging.
3. Add a second output branch.

This incremental path keeps debugging simple.
