# Escalation And Approval Flow

Some tasks need more access than normal. Handle that with explicit escalation.

## Suggested process

1. Explain exactly what extra access is needed.
2. Approve only the minimum scope.
3. Run the task.
4. Record what changed and why.
5. Remove extra access if it was temporary.

## Approval criteria

Approve an escalation only when all of the following are clear:

- The exact directory or file pattern needed.
- The exact operation type needed (read, write, import, watch, or move).
- Why existing allowlists are insufficient.
- Expected duration (one run, short window, or permanent).
- Validation plan and rollback plan.

## Example

If a migration script needs write access to `./archive/old/`, approve that path only. Do not switch to global allow-all unless there is no safer option.

## Trust modes

`restricted` mode disables higher-risk operations like write, move, import, and watch. Use it when you want safe read-only or low-side-effect behavior.

`trusted` mode allows full language behavior, still bounded by policy paths and deny globs.

## Good escalation habits

- Prefer adding one path over flipping `allow_all` to `true`.
- Prefer one-run CLI overrides over editing a shared policy when urgency is temporary.
- Pair escalation with a follow-up task to remove or narrow it.
- Keep escalation changes in version control with short rationale notes.

## Incident response pattern

When troubleshooting active production issues:

1. Create a short-lived branch or patch with the minimal policy expansion.
2. Run the recovery workflow.
3. Revert or narrow immediately after recovery.
4. Add a postmortem note describing the root cause and permanent policy update (if any).
