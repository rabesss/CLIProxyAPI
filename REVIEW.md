# REVIEW.md

## Review Contract
- Review only issues introduced by the current pull request unless a changed line exposes an existing critical risk.
- Prioritize correctness, security, data integrity, auth, destructive operations, and user-visible regressions.
- Keep comments actionable: name the failing behavior, the affected path, and the smallest credible fix.
- Do not flag formatting-only issues, dependency freshness, or broad refactors when existing tooling or maintainers already own them.

## Severity Calibration
- Critical: data loss, privilege escalation, token or secret exposure, billing mistakes, broken authentication, destructive operations without safeguards.
- Warning: missing validation, unsafe defaults, untested edge cases, concurrency/race risks, resource leaks, misleading errors.
- Nit: avoid unless the issue materially affects maintainability or repeated review history says this repo needs attention there.

## Verification Expectations
- Business logic changes need tests that assert observable behavior.
- Security-sensitive changes need negative tests or clear manual verification notes.
- CLI/API changes should preserve backward compatibility unless the PR explicitly documents a breaking change.
- Generated files, snapshots, and lockfiles should only be reviewed when they are the actual source of risk.

## Agent-Maintained Review Memory
Agents that open or update PRs in this repository must keep this section current when review history shows a repeated pattern. Add dated bullets only for durable repo-specific lessons, not one-off PR commentary.

- No recurring repo-specific review patterns have been recorded yet.
