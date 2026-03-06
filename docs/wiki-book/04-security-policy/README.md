# Chapter 4: Security Policy and Permissions

This chapter covers how to run Loom safely in real projects.

Security in Loom is intentionally layered:
- A **policy file** limits which paths can be read, written, imported, or watched.
- A **trust mode** (`trusted` vs `restricted`) controls higher-risk operations.
- **Deny globs** provide explicit block rules, even when a path is otherwise allowed.

Use this chapter to build a policy that is strict by default, easy to review, and simple to evolve.

## What you will learn

- How to apply least privilege to real Loom workflows.
- How each `.loomrc.json` field maps to runtime behavior.
- How to handle temporary escalation without normalizing broad access.
- How to roll out a team policy with clear ownership and rollback.

## Recommended reading order

1. [Least Privilege Basics](01-least-privilege-basics.md)
2. [Filesystem And Command Controls](02-filesystem-and-command-controls.md)
3. [Escalation And Approval Flow](03-escalation-and-approval-flow.md)
4. [Team Policy Checklist](04-team-policy-checklist.md)
