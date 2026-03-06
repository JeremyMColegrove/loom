# Reviewing Changes Safely

A successful run is not enough. Review the result like production code.

## 1) Inspect generated outputs

Check:
- Did expected files get created?
- Are row counts and columns reasonable?
- Did any file get overwritten unexpectedly?

## 2) Validate error behavior

Force a failure case on purpose (bad path, malformed CSV, or denied path).

Confirm:
- `on_fail` behavior runs.
- Error logs are useful.
- Partial writes are handled correctly.

## 3) Re-run for determinism

Run the same script multiple times with same input.

Check if output is stable and duplicates are handled as expected.

## 4) Use policy errors as signal

If Loom reports unauthorized read/write/import/watch, do not disable policy blindly.

Instead, decide whether the new access is truly required, then update policy narrowly.

## Practical review checklist

- Script readable in one pass.
- Side effects are explicit.
- Policy is minimal.
- Failure path is tested.
