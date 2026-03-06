# Setup And First Run

This section gets you to a working first execution.

## Step 1: Create a minimal workspace

Suggested starter layout:

```text
my-loom-project/
  scripts/
    first.loom
  inbox/
  out/
  logs/
  .loomrc.json
```

## Step 2: Add a policy file

Create `.loomrc.json` in the project root:

```json
{
  "version": 1,
  "trust_mode": "trusted",
  "allow_all": false,
  "read_paths": ["./inbox", "./scripts"],
  "write_paths": ["./out", "./logs"],
  "import_paths": ["./scripts"],
  "watch_paths": ["./inbox"],
  "deny_globs": ["**/*.pem", "**/*.key"]
}
```

This gives Loom only the folders needed for the starter workflow.

## Step 3: Add your first script

Create `scripts/first.loom`:

```loom
@read(\"./inbox/sample.csv") >> @csv.parse >> filter(row >> row.price > 1000) >> "./out/high-value.csv" on_fail as err >> \"error: " + err >> "./logs/errors.log"
```

## Step 4: Run Loom

From project root:

```bash
loom scripts/first.loom
```

If you use a custom policy path:

```bash
loom --policy ./.loomrc.json scripts/first.loom
```

## What success looks like

- No parse or validation error.
- Output file appears in `./out/`.
- Log file stays empty unless a failure happens.
