# Filesystem And Command Controls

Loom uses policy controls to authorize file operations.

A common policy file is `.loomrc.json` next to your script.

Example:

```json
{
  "version": 1,
  "trust_mode": "trusted",
  "allow_all": false,
  "read_paths": ["./inbox", "./shared"],
  "write_paths": ["./out", "./logs", "./archive", "./quarantine"],
  "import_paths": ["./lib"],
  "watch_paths": ["./inbox"],
  "deny_globs": ["**/secret.txt", "**/*.pem"]
}
```

## Field meanings

- `version`: policy schema version. Current value is `1`.
- `allow_all`:
  - `false`: only explicitly listed paths are allowed.
  - `true`: capabilities default to full filesystem scope unless that capability has an explicit path list.
- `trust_mode`: `trusted` or `restricted` (also overridable by CLI flag).
- `read_paths`, `write_paths`, `import_paths`, `watch_paths`: allowlists for each capability; each entry can be a literal root or a glob pattern (for example `./inbox_*`).
- `deny_globs`: explicit deny rules that override allowlists.

## Resolution rules that matter in practice

1. Relative paths in policy are resolved from the policy file directory.
2. `"*"` inside a path list means filesystem root (`/` on Unix-like systems).
3. `deny_globs` are checked before allowlist membership and always win.
4. If `allow_all` is `true` and only one capability has an explicit list, that one capability is constrained while the others stay broad.

## Trust mode vs policy

Trust mode and path policy are independent controls:

- `trusted` mode: full runtime features, still bounded by paths and deny globs.
- `restricted` mode: blocks write, move, import, and watch operations regardless of path allowlists.

Use path policy for *where* operations can happen, and trust mode for *which operation types* are allowed.

## Policy discovery and overrides

When running `loom script.loom`, policy resolution is:

1. `--policy <file>` if provided.
2. `.loomrc.json` next to the script.
3. `.loomrc.json` in current working directory.

Useful overrides:

- `--trust-mode trusted|restricted` overrides policy trust mode for that run.
- `--require-policy` fails the run if no policy file is found.
- `LOOM_REQUIRE_POLICY=0` disables policy-required default behavior.

## Recommended workflow

1. Start with only the paths you know are needed.
2. Keep `allow_all` set to `false` for routine workloads.
3. Add deny globs for secrets and credentials early.
4. Run scripts and tests.
5. Expand narrowly when authorization errors are expected and legitimate.
