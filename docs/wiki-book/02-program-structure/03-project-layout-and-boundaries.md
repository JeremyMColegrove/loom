# Project Layout And Boundaries

Good Loom projects separate scripts, modules, and data paths clearly.

## Suggested layout

```text
project/
  scripts/
    ingest.loom
    cleanup.loom
  lib/
    logic.loom
    formatters.loom
  policies/
    ingest.loomrc.json
  inbox/
  out/
  archive/
  quarantine/
  logs/
```

This layout keeps code and runtime data separate.

## Boundary rules that scale

- Keep imports in `lib/` or another dedicated module folder.
- Keep policy files versioned in source control.
- Keep transient runtime data (`inbox`, `out`, `logs`) out of module directories.
- Keep one script focused on one workflow domain.

## Why boundaries matter

Without boundaries, scripts become hard to reason about:
- Imports become unclear.
- Policy allowlists become too broad.
- Side effects leak across unrelated jobs.

With boundaries, policy is easier to write and review.

## Example boundary-friendly import

```loom
@import "lib/logic" as util
@read(\"./inbox/orders.csv") >> @csv.parse >> filter(row >> util.is_valid(row)) >> "./out/orders-clean.csv"
```

This makes module code reusable while keeping IO paths explicit and policy-friendly.
