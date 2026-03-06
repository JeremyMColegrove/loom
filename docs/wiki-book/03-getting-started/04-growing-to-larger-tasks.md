# Growing To Larger Tasks

After a few small successes, scale by structure, not by script size.

## Phase-based growth

Use phases for bigger work:
1. Ingest and parse.
2. Validate and normalize.
3. Route to final destinations.
4. Archive or quarantine.

Each phase should be testable on its own.

## Move shared logic into modules

When logic repeats, extract helpers:

```loom
@import "scripts/logic" as util
@read(\"./inbox/orders.csv") >> @csv.parse >> filter(row >> util.is_valid(row)) >> "./out/clean-orders.csv"
```

This keeps main flows short and easier to review.

## Add operational safeguards

For larger workflows, add:
- `@atomic` around multi-step side effects.
- explicit `on_fail` recovery branches.
- clear log output for troubleshooting.

## Scaling rule of thumb

If one script becomes hard to explain in two minutes, split it into modules or multiple staged scripts.

Readable structure is a feature, not extra work.
