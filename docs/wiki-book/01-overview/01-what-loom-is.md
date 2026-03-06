# What Loom Is

Loom is a small language focused on one job: file processing pipelines.

Instead of writing many lines of setup code, Loom lets you describe a flow from left to right:

```loom
@read(\"./inbox/orders.csv") >> @csv.parse >> filter(row >> row.amount > 1000) >> "./out/high-value.csv"
```

You can read this like a sentence:
- Read a file.
- Parse CSV.
- Keep only high-value rows.
- Write the result.

Important syntax note:
- Plain quotes (`"..."`) are file/path literals in Loom.
- In argument positions, plain quotes cause Loom to read that file and pass file contents.
- Use escaped strings (`\"..."`) for literal text values.

## What Loom is optimized for

Loom is optimized for workloads like:
- Watching folders for new files.
- Parsing structured text (especially CSV).
- Filtering and transforming records.
- Routing files and results to different destinations.
- Recovering safely when something fails.

## Core language ideas

Loom has a few core ideas that show up everywhere:
- Pipelines with `>>` so data flow is visible.
- Directives with `@` for operations like `@watch`, `@read`, `@csv.parse`, and `@atomic`.
- Branches with `[...]` so one input can feed multiple outputs.
- Error handlers with `on_fail` so failures have explicit recovery behavior.

## Why this is useful

In many scripting tasks, logic is simple but plumbing is noisy. Loom reduces that plumbing so the script describes intent instead of setup code.
