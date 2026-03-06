# First End-to-End Example

This example shows a realistic pipeline using watch, parse, filter, branch, and recovery.

```loom
@watch("./inbox/") as event >>
filter(event.type == "created" && event.file.ext == "csv") >>
@atomic >> [
    @read(event.file.path) >> @csv.parse as data >> [
        filter(row >> row.price > 1000) >> "./reports/premium.csv",
        filter(row >> row.price > 0) >> "./reports/all-valid.csv"
    ],
    event.file -> "./archive/"
] on_fail as err >> [
    "processing failed: " + err >> "./logs/error.log",
    event.file -> "./quarantine/"
]
```

## What happens step by step

1. `@watch("./inbox/")` emits file events.
2. `filter(...)` keeps only new CSV files.
3. `@atomic` starts a transaction-like boundary.
4. File content is read and parsed as CSV.
5. Rows are branched into two output files.
6. Source file is moved to archive on success.
7. `on_fail` logs and quarantines on error.

## Why this example matters

This is the core Loom value in one script:
- Clear data flow.
- Explicit side effects.
- Built-in recovery path.
- Minimal boilerplate.

In the next chapters, we break down each part in detail.
