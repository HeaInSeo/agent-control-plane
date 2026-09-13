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
- The `idempotency_key` is derived from the whole intent, and the store always
  recomputes it rather than trusting a supplied value, so a retry is the same
  publication and no caller can mint a second identity for one intent.
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
- Completion requires the evidence to come from the task's current,
  non-terminal attempt with an unreleased workspace, so a fenced-out attempt
  cannot complete a task a successor is still running. The task's
  current-attempt pointer only moves forward to a live attempt, so the guard
  cannot be undone by moving the pointer back.
- Completion is final: re-completing with different evidence is refused, and
  the schema forbids rebinding the completion evidence once set.
- A terminal attempt cannot be revived, so a status marked FAILED, ABANDONED
  or EVIDENCE_UNKNOWN cannot re-enter the modifying slot or reacquire the
  ability to complete anything.
- A publication that was recorded `REJECTED` cannot be flipped back to
  `APPLIED` and then used to complete a task; `OBSERVED` and `REJECTED` are
  terminal and nothing returns to `PENDING`. Nor can an `APPLIED` publication
  regress to `UNKNOWN` and from there be recorded `REJECTED`, which would
  leave a publication that actually landed permanently recorded as never
  having happened.
- A publication is always recorded at `PENDING`, so no row can be inserted
  already claiming an observed effect.
- A withdrawn task cannot mint a publication, a workspace or an observation.
  Withdrawal leaves the attempt live, so nothing else in the chain notices —
  and a cancelled task whose push still went out is the worst outcome this
  control plane can produce.
- A withdrawn or completed task cannot be re-admitted, and a terminal task
  cannot take a new attempt or change its current attempt.
- The default effect rule is exact SHA equality, not ancestor-or-equal.
- For modifying work, completion additionally requires a publication binding
  whose status is `APPLIED` or `OBSERVED`.
- For read-only work, completion requires `READ_ONLY_REVIEW` evidence bound to
  a fixed `reviewed_sha` and an `artifact_digest`, with no publication
  reference. Generic `BRANCH_HEAD` or `PULL_REQUEST_HEAD` evidence cannot
  complete a read-only task: review work publishes nothing, so equal
  published/observed SHAs are self-selected and establish nothing. The two
  kinds of evidence cannot carry each other's binding, in either direction,
  even partially.
- Evidence rows are immutable once written, which is what makes the artifact
  digest a binding rather than a note.
- A workspace release is final: it cannot be reset to live, which would defeat
  both released-workspace guards, nor overwritten, which would lose when the
  workspace stopped being usable.

## Stale scheduler acting as the owner

The attack: a scheduler process that lost ownership — after a restart,
partition or takeover — continues to admit attempts or publish.

Defences in M0:

- Ownership is a singleton row whose `current_epoch` may only move forward.
- Attempts, publications, evidence observations, completions and appended
  history must all bind the current epoch. A retired generation's `(epoch,
  seq)` stream is closed, so a superseded scheduler cannot add records to the
  replay stream. Admission and publication are enforced in Go and by
  trigger; evidence and completion are enforced in Go, because the schema can
  only see that an observation matches its own attempt's epoch, which a
  retired attempt satisfies trivially.
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
- Every authority-bearing column of a stored packet is immutable after
  approval — not only the source binding and scope, but also lane, intent,
  repository subject, approval and expiry times, stop conditions and the
  acceptance contract. Only `status` moves.
- `status` moves only along `APPROVED -> STALE`, `APPROVED -> SUPERSEDED` or
  `STALE -> SUPERSEDED`. A packet that lost authority never regains it, so a
  stopped execution cannot be re-authorised by a status write.

An immutable source snapshot may later be given to a worker as explanatory
context. It must not become a second runtime authority.

## Durable state corruption

The attack, or accident: a damaged, foreign or newer-than-supported database is
silently replaced or adopted, destroying the execution history the control
plane exists to hold.

Defences in M0:

- A missing database is an error unless creation is explicitly requested.
- A non-SQLite file is rejected and left untouched.
- A SQLite file without this control plane's `db_kind` marker, or declaring a
  `db_contract` this build does not implement, is refused — and refused
  without being touched: every inspection happens before the first
  write pragma, so a mistyped path cannot convert an unrelated service's
  database to WAL on the way to rejecting it.
- The integrity check runs on open and a failure is terminal.
- A schema newer than the running build fails closed.
- A tampered migration checksum, or a recorded migration this build does not
  know, fails closed — on every open, not only when migrating.
- Bootstrap writes the identity marker and the migration ledger in one
  transaction before any migration, so a crash mid-bootstrap leaves a database
  that is recognisably ours and safely retryable instead of one that fails as
  foreign and can only be recovered by deleting the history. Bootstrap is
  idempotent and never adopts a file whose marker names another owner.
- Each migration and its ledger entry share one transaction, so a failed step
  leaves neither its schema changes nor a record claiming it succeeded.
- Event history rejects `UPDATE` and `DELETE`, and every other durable record
  — packets, tasks, attempts, workspaces, publications, evidence, repository
  subjects, scheduler epochs and the ownership row — rejects `DELETE`, so a
  row cannot be dropped and re-inserted under the same identity with different
  content. A task's packet, intent, lane and repository subject are frozen at
  creation for the same reason: swapping the packet would re-authorise the
  task under a scope it was never approved for.

## Secrets in history

Event fields are redacted on append by key-name matching over token, secret,
password, credential, authorization, bearer, cookie, private key, API key,
access key, session key, SSH key, signature and PAT fragments.

Values are normalised through JSON before redaction, so redaction always runs
on exactly the shape that will be stored. Without that step a Go struct would
pass through untouched and then be marshalled with its json-tagged credential
field intact — the key-name check never saw the key, because in Go it was a
field name rather than a map key.

Redaction descends through every container, not only maps: a sensitive key
nested inside a list, inside a list of lists, or inside a typed Go slice of
maps is redacted too, so a shape like

```json
{"items": [{"authorization": "..."}]}
```

cannot reach durable history with its credential intact. A sensitive key whose
value is itself a container has the whole value replaced rather than being
descended into. Redaction happens inside the store, so a caller that passes a
credential cannot get it into durable history.

Coverage includes auth, pass, pw, passphrase, deploy/signing/encryption keys,
JWTs and one-time codes, in every spelling: multi-word names are matched
against the separator-normalised form as well as the raw one, so `api_key`,
`apiKey`, `api-key` and the canonical header form `x-api-key` are all covered.
Matching only the raw string let the hyphenated spellings — the ones that
actually appear in HTTP headers and config files — through. Short sensitive tokens are matched as whole words
rather than substrings.
`pat` occurs inside path, patch, compat, pattern and dispatch; `auth` inside
author, authored_at and authority; `pass` inside passing and bypass. Because
redaction runs inside `AppendEvent` on an append-only table, a substring match
there would permanently destroy ordinary operational data — the workspace path
of every attempt, for one — from the history this control plane exists to
preserve. Recursion is depth-bounded, and a self-referential value is a clean
error rather than a crash.

A standalone credential word is treated as sensitive wherever it appears, so
a name built around one — `auth-mode`, `pass-through-count` — is redacted too.
That is deliberate: none of those words is part of the event vocabulary this
control plane writes, and for a name that reads as a bare credential word the
safe reading is that it holds one.

This is defence in depth, not a licence to pass secrets: key-name matching
cannot catch a credential stored under an innocuous name, one embedded in a
free-text message, one sitting as a bare element of a list where there is no
key to match on, or a short token buried in an unseparated acronym run such as
`GITHUBPATValue` — splitting that needs a dictionary. Every separated or
camelCased spelling is covered.

## Known gaps at M0

| Gap | Required before |
| --- | --- |
| A completed task still holds its repository's modifying slot — escalated, see below | resolution before M1 admission |
| Workspace symlink resolution — the stored-path guarantee is lexical only | the workspace allocator (M2) |
| OS-level worker isolation and credential fence | first real worker (M2) |
| Worker push-isolation test | first real worker (M2) |
| Publisher with exact-SHA enforcement | first remote mutation (M3) |
| GitHub reconciliation of repository identity and effects | autonomous admission |
| Lease expiry, takeover and crash recovery | autonomous execution |
| Repository governance on this repository itself | autonomous mutation of it |

## Open architecture contradiction

**A successfully completed task leaves a live attempt holding its
repository's modifying slot.**

Completion requires the evidence to come from a non-terminal attempt, which is
what stops a fenced-out attempt from completing work a successor is running.
But `WorkerAttemptStatus` has no success-flavoured terminal state: `FAILED`,
`ABANDONED` and `EVIDENCE_UNKNOWN` all describe a failure. So after a task
completes, its attempt is still live and still occupies the single per-
`RepositorySubject` modifying slot.

To be precise about the consequence: the repository is **not** permanently
wedged. Retiring the completing attempt does free the slot, and is permitted.
But the only statuses available for that are failure statuses, so freeing the
slot costs a false durable record — the attempt that succeeded goes on record
as abandoned, in a table that rejects `UPDATE` and `DELETE`. The defect is
that the control plane's own history has to be falsified to keep scheduling,
not that scheduling stops.

Two candidate resolutions, both of which change a contract specified in the
M0 packet:

1. Add a terminal, non-failure `WorkerAttemptStatus` (a `SUCCEEDED`), which
   extends a state domain the packet enumerates.
2. Redefine the CC9 slot as "a live attempt of a non-terminal task", which
   means the exclusion can no longer be a partial unique index — it becomes a
   trigger that joins to `task_run`.

Neither is taken unilaterally. `TestCompletedTaskStillHoldsModifyingSlot_KnownEscalation`
pins the current behaviour — including the escape hatch and its cost — so the
gap is visible rather than latent, and will fail if the behaviour changes.
