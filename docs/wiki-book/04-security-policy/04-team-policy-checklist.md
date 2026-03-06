# Team Policy Checklist

Before team rollout, decide:
- Allowed directories.
- Allowed command prefixes.
- Required tests/checks.
- Review and rollback process.

Write this policy down and keep it versioned.

## Baseline decisions (required)

1. Default trust mode (`restricted` or `trusted`) for CI and local runs.
2. Required policy file location (`.loomrc.json` next to script, repo root, or both).
3. Whether missing policy should fail by default (`--require-policy` / `LOOM_REQUIRE_POLICY`).
4. Allowed read, write, import, and watch path boundaries for each workflow class.
5. Standard deny globs for secrets and credentials.

## Approval model

Define this once so escalations are consistent:

1. Who can request temporary access.
2. Who can approve temporary access.
3. Maximum duration before re-approval is required.
4. Required audit note format (ticket ID, reason, affected paths, expiry).

## Verification gates

Require at least:

1. A test or fixture proving out-of-scope reads are blocked.
2. A test or fixture proving out-of-scope writes are blocked.
3. A test or fixture proving `deny_globs` block sensitive targets.
4. A smoke run for each production script with current policy.

## Operational checklist template

Use this in `docs/security-policy-checklist.md` (or equivalent):

```md
# Loom Security Rollout Checklist

## Scope
- [ ] Workflows in scope are listed.
- [ ] Owners are listed for each workflow.

## Policy
- [ ] `trust_mode` default is documented.
- [ ] `allow_all` usage policy is documented.
- [ ] `read_paths`/`write_paths`/`import_paths`/`watch_paths` are documented per workflow.
- [ ] `deny_globs` baseline is documented.

## Escalation
- [ ] Request + approval roles are documented.
- [ ] Max escalation duration is documented.
- [ ] Rollback procedure is documented.

## Verification
- [ ] Unauthorized read test exists.
- [ ] Unauthorized write test exists.
- [ ] deny_globs test exists.
- [ ] CI runs checks before deploy.
```

## Command controls in surrounding tooling

Loom policy governs filesystem and operation authorization. If your team uses wrappers (CI jobs, task runners, or agent environments), define command prefix allowlists there as a separate control.
