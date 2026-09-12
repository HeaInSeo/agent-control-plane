# Threat boundary — M0

This document states the trust boundaries the control plane is built around.
Some are enforced in M0. Others are contracts that must be enforced before a
specific later milestone, and are written down now so that milestone cannot
quietly skip them.

## Trust model

```text
┌─────────────────────────────────────────────┐
│ Scheduler / Publisher   PRIVILEGED          │
│   scheduler SQLite database                 │
│   publisher credentials (later milestone)   │
│   control socket (later milestone)          │
└─────────────────────────────────────────────┘
                    │  approved packet + workspace
                    ▼
┌─────────────────────────────────────────────┐
│ Worker                  UNPRIVILEGED        │
│   its own attempt workspace                 │
│   its approved packet and inputs            │
│   its tool and runtime access               │
└─────────────────────────────────────────────┘
```

The worker is treated as a capable, non-malicious, and *not trusted* process.
It runs arbitrary code, including arbitrary local Git, and it may be wrong
about what it did. Nothing it says is evidence.

## CC2 — worker isolation contract

Required invariant:

```text
Scheduler / Publisher = privileged execution identity
Worker                = separate unprivileged identity OR OS-enforced sandbox
```

A worker must be denied:

- the scheduler SQLite database,
- publisher or GitHub write credentials,
- any other attempt's workspace,
- the scheduler control socket,
- any unrelated repository checkout.

The credential fence must remove or neutralise inherited paths, not merely
unset a few variables: Git and GitHub tokens, the SSH agent socket, Git
credential helpers, global Git configuration, and write-capable remotes.

**Status: contract only.** M0 launches no worker, so there is nothing to
sandbox yet. Environment-variable hygiene alone is explicitly *not* sufficient:
a worker running as the same unrestricted OS identity as the scheduler can read
the scheduler database and the publisher's credentials regardless of what the
environment says. Enforcement, with a push-isolation test, is required before
the first real worker runs.

## Confused deputy on publication

The attack: a worker arranges for the scheduler to publish something other
than the reviewed result of the worker's own attempt — a different commit, a
different ref, a different repository, or the result of a stale attempt that
was already fenced out.

Defences in M0:

- A publication intent binds `task_id`, `attempt_id`, `scheduler_epoch`,
  `fence_epoch`, `workspace_id`, `repository_subject_id`, `base_sha` and
  `source_commit_sha`. Triggers require every one of those to agree with the
  attempt row and the workspace row.
- `source_commit_sha` must be a full 40-hex commit name. A branch name, a
  symbolic ref or an abbreviation cannot be stored.
- `target_ref` must be a fully qualified, non-symbolic `refs/...` name.
- The `idempotency_key` is derived from the whole intent, so a retry is the
  same publication and a different intent cannot reuse an existing identity.
- A publication may only be recorded under the current scheduler epoch.
- Only a `MODIFYING` attempt may record a publication.
- `domain.CheckPublishPreconditions` re-checks the binding against live state,
  including that the workspace has not been released.

Deferred: the publisher itself, and therefore the actual credential handling.
Nothing in M0 holds a GitHub write capability.

## Fabricated or borrowed completion

The attack: a task is marked complete without the approved effect existing —
by a worker exiting zero, by a worker saying so, by reusing another task's
evidence, or by observing a ref that happens to have moved for unrelated
reasons.

Defences in M0:

- No function maps a process exit code or a self-report to a task status.
- `state.TaskCompleted` is reachable only via `domain.DeriveTaskCompletion`.
- `store.SetTaskRunStatus` explicitly refuses `COMPLETED`.
- The schema requires `(status = 'COMPLETED') = (completed_evidence_id IS NOT NULL)`,
  and a trigger requires that evidence to be attributed to that very task.
- Evidence must match the attempt on all of task, attempt, scheduler epoch,
  fence epoch, repository subject and workspace.
- The default effect rule is exact SHA equality, not ancestor-or-equal.
- For modifying work, completion additionally requires a publication binding
  whose status is `APPLIED` or `OBSERVED`.
- Evidence rows are immutable once written.

## Stale scheduler acting as the owner

The attack: a scheduler process that lost ownership — after a restart,
partition or takeover — continues to admit attempts or publish.

Defences in M0:

- Ownership is a singleton row whose `current_epoch` may only move forward.
- Attempts and publications must bind the current epoch, enforced in Go and by
  trigger.
- Per-task fencing tokens (`fence_epoch`) are monotonic and unique within a
  task, so a fenced-out attempt cannot reissue its own token.
- Opening the database, checking integrity, verifying the schema or reading
  history never advances the epoch, so a backup tool or an inspector cannot
  hand itself ownership by starting up.

Deferred: actual lease renewal, expiry detection and takeover.

## Repository identity confusion

The attack: a rename, or two repositories sharing an alias, causes work
approved for one repository to run against another — or causes one repository
to appear as two subjects and so obtain two modifying slots.

Defences in M0:

- Identity is `github_node_id`, unique and immutable.
- `owner/name` is an alias, not an identity, and is not unique-keyed.
- Observing a known node id under a new name renames the existing subject and
  records an event.
- Alias lookup fails closed when ambiguous.
- The modifying-exclusion index keys on the subject, so two aliases of one
  subject cannot yield two slots.

Deferred: GitHub reconciliation, which is what would notice a rename in the
first place. Until it exists, identity is only as fresh as what was written.

## Scope creep through live prose

The attack: a worker re-reads the canonical design source at runtime and
expands its own scope, or treats an omission from `forbidden_scope` as
permission.

Defences in M0:

- The approved packet is the execution authority after approval.
- Unspecified actions are denied; forbidden wins over allowed; a
  self-contradictory scope is a validation error.
- An empty `AllowedScope` authorises nothing and fails validation.
- `Authorize` requires the observed `source_revision` and `source_digest` to
  match what was approved. A mismatch is `ErrPacketStale`, a stop condition.
- A stored packet's binding and scope are immutable; only its status moves.

An immutable source snapshot may later be given to a worker as explanatory
context. It must not become a second runtime authority.

## Durable state corruption

The attack, or accident: a damaged, foreign or newer-than-supported database is
silently replaced or adopted, destroying the execution history the control
plane exists to hold.

Defences in M0:

- A missing database is an error unless creation is explicitly requested.
- A non-SQLite file is rejected and left untouched.
- A SQLite file without this control plane's `db_kind` marker is refused.
- The integrity check runs on open and a failure is terminal.
- A schema newer than the running build fails closed.
- A tampered migration checksum, or a recorded migration this build does not
  know, fails closed.
- Event history rejects `UPDATE` and `DELETE`.

## Secrets in history

Event fields are redacted on append by key-name matching over token, secret,
password, credential, authorization, bearer, cookie, private key, API key,
access key, session key, SSH key, signature and PAT fragments, recursing into
nested maps. Redaction happens inside the store, so a caller that passes a
credential cannot get it into durable history.

This is defence in depth, not a licence to pass secrets: key-name matching
cannot catch a credential stored under an innocuous name, or one embedded in a
free-text message.

## Known gaps at M0

| Gap | Required before |
| --- | --- |
| OS-level worker isolation and credential fence | first real worker (M2) |
| Worker push-isolation test | first real worker (M2) |
| Publisher with exact-SHA enforcement | first remote mutation (M3) |
| GitHub reconciliation of repository identity and effects | autonomous admission |
| Lease expiry, takeover and crash recovery | autonomous execution |
| Repository governance on this repository itself | autonomous mutation of it |
