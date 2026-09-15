# Repository guardrail baseline — observed at M0

`HeaInSeo/agent-control-plane` was created for this milestone. It started with
no inherited repository guardrails, and nothing here was assumed: each row was
observed against the GitHub API after creation.

`CONFIGURED` / `NOT CONFIGURED` / `UNVERIFIED` — anything not positively
observed is not treated as present.

| Control | State at M0 | Notes |
| --- | --- | --- |
| CI workflows | `CONFIGURED` | `.github/workflows/ci.yml` added on the M0 branch: gofmt, `go vet ./...`, `go build ./...`, `go test ./...` (CGO_ENABLED=0) and `go test -race`. |
| Branch protection / rulesets | `NOT CONFIGURED` | Repository rulesets list is empty; no protection on `main`. No organisation-level ruleset is readable for this account. |
| Required status checks | `NOT CONFIGURED` | Follows from the absence of protection. A workflow existing is not a required check. |
| Required reviews | `NOT CONFIGURED` | No review requirement; merges are unrestricted. |
| CODEOWNERS | `NOT CONFIGURED` | No CODEOWNERS file, and no enforcement mechanism to honour one. |
| Secret scanning | `CONFIGURED` | Enabled by GitHub default for a public repository, with push protection enabled. Not configured by this milestone. |
| Code scanning | `NOT CONFIGURED` | No code scanning setup or default setup. |
| Dependabot security updates | `NOT CONFIGURED` | Disabled. No `dependabot.yml`. |
| Dependabot / vulnerability alerts | `UNVERIFIED` | The alerts endpoint returned 404 for this token; presence could not be positively established either way. |
| Webhooks / other automation | `UNVERIFIED` | Reading repository hooks needs the `admin:repo_hook` scope, which this token does not carry. |
| Auto-merge | `NOT CONFIGURED` | `allow_auto_merge` is false. |
| Delete branch on merge | `NOT CONFIGURED` | `delete_branch_on_merge` is false. |
| Commit signoff requirement | `NOT CONFIGURED` | `web_commit_signoff_required` is false. |

## Why nothing was changed

Server-side governance — branch protection, rulesets, required checks,
required reviews, CODEOWNERS enforcement, merge restrictions, security policy
— was not configured as part of M0. The M0 packet does not authorise changing
repository settings, and doing so unasked would be exactly the kind of
unbounded action this control plane is designed to prevent.

Recommendations are carried in the M0 handoff report for separate decision.

## Milestone distinction

```text
M0   = schema / contracts / store foundation
     = may legitimately begin from an unprotected new repository

M1+  = autonomous execution
     = must NOT assume repository governance exists
     = the guardrail baseline must be reviewed before any autonomous mutation
```
