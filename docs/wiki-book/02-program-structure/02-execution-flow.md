# Execution Flow

Loom execution follows a clear lifecycle. Understanding this helps with debugging and script design.

## Stage 1: Parse

The parser reads `.loom` source and builds a program structure.

If syntax is invalid, execution stops with line/column parse errors.

## Stage 2: Validate

Before runtime execution, validation checks for issues.

In strict mode, warnings can fail execution. This helps teams keep scripts clean and predictable.

## Stage 3: Initialize runtime

Runtime is created with:
- Script directory context (used for imports and path resolution).
- Security policy (`.loomrc.json` or explicit `--policy`).
- Trust mode (`trusted` or `restricted`).

## Stage 4: Execute flows

For each flow:
1. Evaluate source.
2. Pass value through each destination operation.
3. Apply operator behavior (`>>` append, `>>>` overwrite, `->` move).
4. If a step fails, evaluate `on_fail` when provided.

`@atomic` can wrap operations in a transaction-like scope. If a later step fails, writes made inside that scope are rolled back.

## Stage 5: Final outcome

Execution exits with success or a runtime error message.

For long-running watcher flows, shutdown signals stop active watchers cleanly.

## Example lifecycle in one script

```loom
@watch(\"./inbox/") as event >> filter(event.type == \"created") >> @read(event.file.path)
```

This parses, validates, initializes policy/trust context, then runs as a watcher-driven flow.
