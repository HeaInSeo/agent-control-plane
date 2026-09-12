# Architecture — M0

This document describes what M0 builds and, as importantly, what M0 refuses to
decide yet.

## Planes

```text
Canonical Design Source        semantic authority
        ↓ packetize
ExecutionPacket Candidate
        ↓ explicit approval
Approved ExecutionPacket       bounded execution authority for one execution
        ↓
Scheduler                      ownership, admission, publication
        ↓
WorkerAttempt                  one bounded execution inside one workspace
```

The control plane owns deterministic scheduling, durable execution state,
claim and fencing state, worker attempts, workspace lifecycle, scheduler-owned
publication, completion evidence and crash recovery.

It does not own product semantics for the repositories it operates on, product
architecture decisions, VM or cluster lifecycle, GitHub's own repository truth,
or design authority.

## Deployment profile (v0.1)

```text
ONE HOST · ONE ACTIVE SCHEDULER · ONE SQLITE DATABASE

Operator modifying WIP   <= 2
Guardrail modifying WIP  <= 2
Review / read-only WIP   <= 3

Per RepositorySubject: active modifying attempts across ALL lanes <= 1
```

M0 implements the storage and identity foundation for this profile. The WIP
ceilings themselves are scheduler-loop policy and are not enforced in M0; the
per-repository modifying exclusion *is* already enforced, because it is an
identity constraint rather than a policy knob (see CC9 below).

## Entities

| Entity | Identity | Role |
| --- | --- | --- |
| `SchedulerEpoch` | `epoch` (monotonic integer) | Scheduler ownership generation. |
| `RepositorySubject` | `github_node_id` | Stable repository identity; `owner/name` is an alias. |
| `ExecutionPacket` | `packet_id` | Closed-world bounded execution authority. |
| `TaskRun` | `task_id` | The durable scheduling subject. |
| `WorkerAttempt` | `attempt_id` | One bounded execution, fenced within its task. |
| `Workspace` | `workspace_id` | Filesystem identity owned by exactly one attempt. |
| `PublishAttempt` | `publish_attempt_id` + `idempotency_key` | Intent to mutate a remote, bound to an exact commit. |
| `EvidenceObservation` | `evidence_id` | Externally observed repository effect. |
| `Event` | `(scheduler_epoch, seq)` | Append-only history. |

Deferred to later milestones, with stable endpoint identifiers already
available for them: `UsageObservation`, `ProjectionEvent`, `Finding`,
`DependencyEdge`, `SuccessorEdge`, and a `CompletionEvidenceContract` table.

Lease and checkpoint are fields and events on `WorkerAttempt` rather than
first-class entities. `RepoLock` is deliberately absent: modifying admission is
a derived constraint over current attempts, not a separate source of truth
that could disagree with them.

## State domains (CC6)

There is no single status enum. Four domains, four distinct Go types, four
independent `CHECK` constraints:

| Domain | Values |
| --- | --- |
| `PacketStatus` | `APPROVED`, `STALE`, `SUPERSEDED` |
| `TaskRunStatus` | `READY`, `RUNNING`, `BLOCKED_DESIGN`, `COMPLETED`, `ABANDONED` |
| `WorkerAttemptStatus` | `CLAIMED`, `STARTING`, `RUNNING`, `VERIFYING`, `FAILED`, `ABANDONED`, `EVIDENCE_UNKNOWN` |
| `PublishStatus` | `PENDING`, `APPLIED`, `OBSERVED`, `REJECTED`, `UNKNOWN` |

Two absences are load-bearing. `PACKET_STALE` is not a worker attempt state —
packet staleness is a property of the authority, not of the process. And
`COMPLETED` is not a worker attempt state at all, because completion is a
property of a task derived from evidence.

`Lane` (`operator`, `guardrail`, `review`) is an organisational label and
carries no admission authority. `Intent` (`MODIFYING`, `READ_ONLY`) is what
admission keys on.

## Central corrections as implemented

### CC1 — exact immutable publication binding

`publish_attempt` stores `base_sha`, `source_commit_sha`, `target_ref` and an
`idempotency_key` derived from the whole intent. The column constraints require
a 40-hex lowercase commit name and a fully qualified, non-symbolic `refs/...`
target, so "publish branch HEAD" cannot be represented, let alone executed.
`domain.CheckPublishPreconditions` is the gate the future publisher must pass:
current epoch, current attempt, matching fence, live attempt, owning workspace,
unreleased workspace, matching repository subject, commit present in workspace.

M0 records intent. It does not push.

### CC2 — worker isolation boundary

A contract in this milestone, enforced before the first real worker. See
`docs/threat-boundary.md`.

### CC3 — workspace isolation

`IsolationKind` offers `ISOLATED_CLONE` and `READ_ONLY_CHECKOUT`. A shared Git
worktree is not in the set, because worktrees share Git metadata with a process
that can run arbitrary local Git. The schema holds one workspace per attempt
(`UNIQUE(attempt_id)`), one attempt per directory (`UNIQUE(root_path)`), and an
immutable owner, so a workspace cannot be shared or rebound.

`root_path` must be absolute and already canonical. Uniqueness of a string is
not uniqueness of a directory: `/a/ws`, `/a/ws/`, `/a/./ws`, `/a/b/../ws` and a
relative `ws` are five distinct strings naming at most one directory, so
without canonicalisation the constraint would not mean what it says.

### CC4 — SchedulerEpoch

`ActivateScheduler` is the only path that inserts an epoch. `Open`,
`IntegrityCheck`, `VerifySchema`, `AppliedMigrations`, `Migrate` and every
read-only inspection leave the epoch untouched. `scheduler_ownership` is a
singleton row whose `current_epoch` may only move forward, and attempts and
publications must bind the current epoch — checked in Go and again by trigger.

Old-epoch rows stay readable for ever. They never regain ownership.

### CC5 — RepositorySubject

`github_node_id` is `UNIQUE` and immutable; `current_full_name` is an alias
with an ordinary index. Observing a known node id under a new name renames the
existing subject and records a `repository_subject.renamed` event, so a rename
cannot fork one repository into two subjects. Alias lookup fails closed when
ambiguous, which is the state a recent rename produces.

M0 has no GitHub reconciliation client. Identity is stored and validated; it is
not yet fetched.

### CC6 — state ownership separation

See the table above.

### CC7 — evidence binding

`evidence_observation` carries the full attempt identity and is checked against
the attempt row before insert and by trigger. The default rule is exact
equality of `observed_sha` and `published_sha`. Generic ancestor-or-equal is
rejected as a default: an unrelated human commit advancing a ref would satisfy
it while the approved effect is no longer the state of the ref. A future effect
that legitimately transforms commits — a merge-generated commit, say — needs
its own explicit evidence contract with provenance, not a loosened default.

Completion additionally requires that the evidence come from the attempt that
currently owns the task, that the attempt is not terminal, and that its
workspace has not been released. Without those checks a fenced-out attempt's
evidence would complete a task a successor was still actively running — the
publication path already revalidated all three, and completion was the
asymmetric hole.

Read-only work has its own evidence contract, because it publishes nothing.
A `READ_ONLY_REVIEW` observation must bind `reviewed_sha` — the fixed commit
the review was performed against — and `artifact_digest`, the digest of the
immutable artifact the review produced. `reviewed_sha` must equal both the
observed and the published SHA, and the observation must reference no
publication.

Generic repository-effect evidence cannot complete a read-only task at all.
For review work nothing is published, so a `BRANCH_HEAD` observation whose two
SHAs agree is self-selected and establishes nothing; `DeriveTaskCompletion`
requires `READ_ONLY_REVIEW` evidence for `READ_ONLY` intent, and rejects
`READ_ONLY_REVIEW` evidence for `MODIFYING` intent. The schema holds the same
contract: one biconditional per review column against the evidence kind, so
neither kind of evidence can carry the other's binding — not even partially.

### CC8 — closed-world ExecutionPacket

`ExecutionPacket.DecideScope` returns allow only for an explicitly allowed
action. Forbidden wins over allowed; unspecified is denied; an empty
`AllowedScope` is a validation error rather than an unbounded world.
`Authorize` re-checks status, expiry and source binding (`source_revision` and
`source_digest`) before launch and before resume.

An approved packet is an immutable execution contract. Every column of
`execution_packet` except `status` is frozen at approval — `task_id`, `lane`,
`intent`, `repository_subject_id`, `source_revision`, `source_digest`,
`packet_digest`, `approved_at`, `expires_at`, `allowed_scope`,
`forbidden_scope`, `stop_conditions` and `acceptance_contract`. A widened
scope, a later expiry, a changed lane or intent, a relaxed acceptance contract
or a removed stop condition are all changes of authority, so none of them is
reachable after approval.

`status` moves only along a permitted transition:

```text
APPROVED -> STALE
APPROVED -> SUPERSEDED
STALE    -> SUPERSEDED
```

Nothing returns to `APPROVED`. Reviving a packet whose binding was already
found not to hold would re-authorise an execution that was stopped for cause;
a new approval is a new packet. Both the Go guard
(`state.PacketStatus.CanTransitionTo`, checked in `SetPacketStatus`) and a
schema trigger enforce this. Deletion is refused too: a packet no task
references yet could otherwise be dropped and re-inserted under the same
identity with a widened scope, which is a content change by another route.

`PublishStatus` has the same shape of rule, because a publication recorded
`REJECTED` that could be flipped to `APPLIED` would be usable to complete a
task:

```text
PENDING  -> APPLIED | OBSERVED | REJECTED | UNKNOWN
APPLIED  -> OBSERVED | UNKNOWN
UNKNOWN  -> APPLIED | OBSERVED | REJECTED
OBSERVED, REJECTED are terminal
```

`PENDING -> OBSERVED` is permitted because a publisher can crash after the
remote mutation lands but before recording `APPLIED`, and later reconciliation
then observes the effect directly. `UNKNOWN` is the one state still open to
resolution — it means the outcome could not be determined, not that it
succeeded, and it never completes a task.

### CC9 — lane-agnostic modifying admission

```sql
CREATE UNIQUE INDEX ux_modifying_slot_per_repository
    ON worker_attempt (repository_subject_id)
    WHERE intent = 'MODIFYING'
      AND status IN ('CLAIMED', 'STARTING', 'RUNNING', 'VERIFYING');
```

The exclusion key is the repository subject, not the lane. No lane can obtain a
private modifying lock, because there is no lane column in the index.

### CC10 — stable identifiers and event identity

One distinct Go type per identifier kind, each with a kind prefix carried in
the stored value, so a misrouted identifier is a compile error in code and
detectable in data. Event order identity is `(scheduler_epoch, seq)`, a
composite primary key, with `seq` allocated by the store inside the appending
transaction.

## SQLite backend

WAL, a single connection matching the single-active-scheduler model, and
`BEGIN IMMEDIATE` via `_txlock=immediate`.

Per-connection pragmas — `foreign_keys`, `synchronous = FULL`, and
`query_only` on read-only handles — are carried in the DSN, not applied once
to the pool. `database/sql` may discard and re-establish its connection at any
time, and a replacement gets only what the DSN carries; a pragma applied once
would silently vanish, taking with it the enforcement of every identity
relation in the schema. `VerifyConnectionPragmas` asserts they are actually in
force rather than assuming it. `journal_mode` is a property of the database
file rather than of the connection, so it is set once.

Open fails closed on: a missing file without explicit `AllowCreate`; a
read-only handle asked to create; a file that is not SQLite; a SQLite file
without this control plane's `db_kind` marker; a failed integrity check. A
corrupt or unexpected database is never silently replaced with a fresh one.

A zero-length file counts as absent for those gates. It is what SQLite leaves
before its first write, but also what a typo or an abandoned run leaves, and
treating it as an existing database would let a mistyped path silently yield a
fresh, empty control plane — the exact case `AllowCreate` exists to prevent.

Migrations are embedded, forward-only and contiguous from version 1, each
recorded with a SHA-256 checksum. Replay is idempotent and doubles as a
consistency check. A database migrated beyond what the running build knows
fails with `ErrSchemaVersionUnsupported` rather than being treated as close
enough. Each migration runs inside one transaction together with its own
ledger entry, so a step can never be recorded as applied when it was not.

Bootstrap — the database identity marker plus the migration ledger — is a
single transaction that runs before any migration. The two must be created
together: a ledger without the marker would be a database with tables and no
marker, which the next open would refuse as foreign, and for a scheduler
database that is unrecoverable without deleting the execution history. With
bootstrap atomic, the only reachable states are

```text
no tables at all            -> fresh, bootstrap it
marker + ledger, 0 applied  -> ours, mid-bootstrap, retry the migrations
marker + ledger, N applied  -> ours, migrate forward from N
```

and bootstrap is idempotent, so a retry after a crash at any point is safe.
Bootstrap never adopts a file: a pre-existing marker declaring another owner
is rejected inside the bootstrap transaction, so foreign-database rejection is
not weakened by the recovery path.

## Not in M0

Worker process launchers, Git push, GitHub PR mutation, publisher credentials,
Notion mutation, real lease renewal or takeover, real GitHub repository
admission or reconciliation, Temporal, multi-host or HA scheduling, Kubernetes
controllers, a web UI, automatic merge or deploy, a workflow DSL, and
LLM-driven scheduling.

No placeholder implementations of these exist either. A fake reconciler, fake
launcher or fake publisher would fix an interface shape before the milestone
that has the information to choose it.
