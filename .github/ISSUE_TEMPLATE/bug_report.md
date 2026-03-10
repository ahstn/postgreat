---
name: Bug Report
about: Capture actionable bugs with reproducible details, impact, and closure criteria.
title: "[Bug]: <concise summary>"
labels: ["bug", "needs-triage"]
assignees: []
---

## Summary

<!--
1-2 sentences.
What is broken and who/what is affected?
-->

## Problem

<!--
Describe the observed issue clearly.
Include where it occurs (route, endpoint, flow, CLI command, etc.).
-->

## Expected Behavior

<!--
Describe what should happen instead.
-->

## Actual Behavior

<!--
Describe what actually happened.
Include visible errors, unexpected output, or incorrect state.
-->

## Reproduction Steps

<!--
Provide deterministic steps with exact inputs when possible.
If this is flaky, describe frequency and known triggers.
-->

1.
2.
3.

## Environment

<!--
Record environment details needed to reproduce quickly.
Use N/A if unknown.
-->

- Commit/Branch:
- Runtime/Tooling versions:
- OS/Browser:
- Config/Env vars:

## Impact

<!--
State severity and blast radius.
Describe user-facing impact, data/cost risk, and blocked workflows.
-->

- Severity:
- Affected scope:
- User-visible impact:

## Evidence

<!--
Paste logs, stack traces, request IDs, screenshots, or payload snippets.
Redact secrets and sensitive data.
-->

```text
Paste relevant logs/errors here
```

## Suspected Area (Optional)

<!--
List likely modules/files if you have a strong lead.
-->

- `crates/...`

## Acceptance Criteria

<!--
Conditions required to close this bug.
Keep each item objective and verifiable.
-->

- [ ] Root cause is identified and documented.
- [ ] Fix resolves the repro path without regressions.
- [ ] Automated test(s) or checks cover the failing scenario.
- [ ] Docs/runbooks are updated if behavior or operation changed.

## Notes

<!--
Include workarounds, rollout/backfill notes, and links to related issues/PRs.
-->
