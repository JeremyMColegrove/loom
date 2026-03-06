# Least Privilege Basics

Least privilege means Loom should only get the exact access needed for the task.

If a script only reads `./inbox` and writes `./out`, do not grant full-disk access.

## Why this matters

File automation is powerful, but side effects are real:
- Overwrites can destroy data.
- Imports can pull unexpected code.
- Watchers can run over sensitive directories.

A tight policy reduces blast radius.

## Practical rule

Start restrictive and open only what is required:
- Read paths for input.
- Write paths for outputs.
- Import paths for trusted modules.
- Watch paths for watcher scripts.

Then verify behavior with tests.

## Capability-by-capability thinking

Do not treat access as one bucket. Decide each capability separately:

- `read_paths`: where source files may come from.
- `write_paths`: where outputs, logs, archives, and quarantines may land.
- `import_paths`: where `@import` modules are allowed.
- `watch_paths`: which directories can be used by watcher flows.

This separation prevents accidental privilege creep. A script that needs broad reads may still need very narrow writes.

## Common anti-patterns

- Reusing one permissive policy for every script.
- Expanding write access to "fix" an import or watch error.
- Leaving temporary emergency exceptions in place after incident handling.
- Assuming `trusted` mode is safe without path boundaries.

## Safer default template

Use a script-local `.loomrc.json` and start from explicit directories:

```json
{
  "version": 1,
  "trust_mode": "trusted",
  "allow_all": false,
  "read_paths": ["./inbox", "./rules"],
  "write_paths": ["./out", "./logs", "./quarantine"],
  "import_paths": ["./scripts"],
  "watch_paths": ["./inbox"],
  "deny_globs": ["**/*.pem", "**/*.key", "**/.env*"]
}
```

## Verification checklist

After policy edits, run a fast check:

1. A normal run succeeds for expected inputs.
2. A read outside `read_paths` is rejected.
3. A write outside `write_paths` is rejected.
4. A denied file pattern in `deny_globs` is rejected.

If all four pass, you have both functionality and boundaries.
