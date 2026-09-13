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
unreleased workspace, matching repository subject, commit present in workspace,
the task still being live, and the packet's authority — status and expiry —
still holding. Packet
authority is checked at all three moments it matters: `Authorize` at launch and
resume, `CreateWorkerAttempt` at admission, and these preconditions at
publication. A packet can go stale, be superseded or expire while an attempt is
mid-run, and a packet that means "execution must stop" must not be the basis
for starting more of it.

The gate also refuses a publication whose lifecycle has already been decided.
That matters most on the recovery path: a publisher restarting after a crash
resolves the existing intent by its idempotency key and re-runs the gate, and
an `APPLIED` or `OBSERVED` intent means the remote mutation already happened.
Resolving an `UNKNOWN` outcome is reconciliation, not re-publication.

Creating a workspace, recording a publication and recording evidence all apply
the same liveness fence as completion, on both the attempt and the task: a
terminal attempt, a released workspace or a finished task cannot be given a
workspace, mint a publication, or have an observation filed against it.

All of these fences — terminal task, terminal attempt and released workspace
— are enforced in the schema as well as in Go — a
`BEFORE INSERT` trigger on `workspace`, `publish_attempt` and
`evidence_observation` — because this file's premise is that a future code
path cannot bypass an invariant by forgetting a check.

The task half matters most for publication. Withdrawing a task leaves its
attempt live — the current-attempt pointer is frozen when a task goes terminal
— so nothing else in the chain notices, and publication is the irreversible
step. A cancelled task whose push still went out is the worst outcome this
control plane can produce, so `CheckPublishPreconditions` carries the task's
scheduling state too. These rows are undeletable and immutable, so
each one handed to a dead attempt is permanent clutter that nothing can ever
act on — and a workspace additionally burns its unique `root_path`.

Recording a publication also revalidates packet authority. The asymmetry with
completion is deliberate: publication *creates* the effect, so it must not
proceed on authority that has lapsed, while completion rests on an effect that
was already externally observed. If the work landed and the packet went stale
afterwards, the change exists in the repository, and refusing to complete
would leave the task open for ever while the effect stands. Publication rows are
undeletable and immutable except for status, so an intent minted by a
fenced-out attempt would sit in the pending queue for ever, resolvable only to
`REJECTED`.

Every timestamp that records "when this row last changed" is written
monotonically, clamped to what the row already holds, and the ordering is a
schema `CHECK` as well. A backward clock step — an NTP correction, a VM
resume — would otherwise store a row claiming it changed before it existed,
and since these rows are immutable or undeletable that claim could never be
corrected. Clamping loses a little precision on a clock glitch and keeps the
invariant, which is the better trade.

A publication also records when its status last moved, and each transition
appends an event. Re-asserting a value is a no-op everywhere it can be
re-asserted — publication, attempt, task and packet statuses, and a task's
current attempt alike. Both the crash-recovery
flow and a periodic reconciler re-assert, and treating that as a move would
rewrite `updated_at` — which means "when the status last moved" — append a
phantom transition per cycle to a table nothing can prune, and let a
long-withdrawn task be made to look freshly touched. A `PENDING` intent has by definition never moved, so its
`updated_at` always equals its `created_at`. `task_run` carries `updated_at` and `workspace` carries
`released_at`, so without this the most irreversible entity would have been
the one recording nothing about when it changed — precisely what a publisher
resuming after a crash needs. Comprehensive per-mutation history for the other
entities is a deliberate deferral rather than an oversight; see the repository
issues.

A publication's identity is also resolvable, not only unique:
`PublishAttemptByIdempotencyKey` and `PublishAttemptsForAttempt` let a
publisher restarting after a crash reach the intent that already exists and
learn its status, which is the half of idempotency that matters during
recovery.

M0 records intent. It does not push.

### CC2 — worker isolation boundary

A contract in this milestone, enforced before the first real worker. See
`docs/threat-boundary.md`.

### CC3 — workspace isolation

The isolation kind is determined by the attempt's intent rather than chosen
freely — `MODIFYING` requires an `ISOLATED_CLONE`, `READ_ONLY` a
`READ_ONLY_CHECKOUT` — because otherwise a modifying attempt could own a
read-only checkout and still publish from it: the publish coherence trigger
checks the attempt's intent, the workspace's owner and its base SHA, but not
its isolation.

`IsolationKind` offers `ISOLATED_CLONE` and `READ_ONLY_CHECKOUT`. A shared Git
worktree is not in the set, because worktrees share Git metadata with a process
that can run arbitrary local Git. The schema holds one workspace per attempt
(`UNIQUE(attempt_id)`), one attempt per directory (`UNIQUE(root_path)`), and an
immutable owner, so a workspace cannot be shared or rebound.

`root_path` must be absolute and already canonical. Uniqueness of a string is
not uniqueness of a directory: `/a/ws`, `/a/ws/`, `/a/./ws`, `/a/b/../ws` and a
relative `ws` are five distinct strings naming at most one directory, so
without canonicalisation the constraint would not mean what it says.

The path must also be deeper than a top-level directory: `/` and `/etc` are
absolute and canonical, and neither is a workspace. Since the allocator
materialises and later releases these trees and `UNIQUE(root_path)` burns
whatever is recorded, a mis-computed root must not be storable. That is a
blast-radius guard rather than a security boundary — it bounds the damage of a
mistake, it does not stop a determined caller naming a bad directory two
levels down.

Symlinks are not covered, and the distinction is worth stating plainly rather
than claiming more than holds: `/srv/ws-a/t1` and `/srv/ws-b/t1` are both
canonical strings naming one physical tree if `ws-a` links to `ws-b`.

Resolving them in the validator was tried and reverted. A pure validator that
does filesystem I/O answers differently for the same path depending on whether
the directory exists yet — accepting a workspace recorded before creation and
rejecting the identical one recorded after, which is backwards for an
allocator that materialises the clone first — and it puts a stat of a
possibly-hung mount inside the write transaction, behind the single
connection. So the guarantee here is lexical, deliberately, and resolving
symlinks belongs to the workspace allocator that creates the directory and can
check the real filesystem once.

### CC4 — SchedulerEpoch

The fence runs in both directions. `RequireCurrentEpoch` asks whether a *row*
belongs to the current generation; `RequireOwnership` asks whether the
*caller* does. Without the second, a process still holding a read-write handle
from a retired generation could drive the current generation's live attempt to
`ABANDONED`, release its workspace, or mark its packet `STALE` — all writes
that target current-epoch rows and so pass every row-level check. A handle
acquires ownership by activating a scheduler and loses it the moment a later
generation activates; a handle that never activated may read but not mutate
execution state.

`ActivateScheduler` is the only path that inserts an epoch. `Open`,
`IntegrityCheck`, `VerifySchema`, `AppliedMigrations`, `Migrate` and every
read-only inspection leave the epoch untouched. `scheduler_ownership` is a
singleton row whose `current_epoch` may only move forward, and attempts and
publications must bind the current epoch — checked in Go and again by trigger.

Old-epoch rows stay readable for ever. They never regain ownership.

### CC5 — RepositorySubject

`github_node_id` is `UNIQUE` and immutable; `current_full_name` is an alias
with an ordinary index. An observation older than or equal to the recorded one
is ignored: GitHub timestamps are second-precision, so two observations in the
same second are ordinary, and "last writer wins" on a tie would revert the
alias and write a backwards rename into append-only history. Observing a known node id under a new name renames the
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
currently owns the task, that the attempt is not terminal, that its workspace
has not been released, and that the attempt's scheduler epoch is current.
Without those checks a fenced-out attempt's evidence would complete a task a
successor was still actively running, and a superseded scheduler generation
could drive a task to COMPLETED on the authority of a retired epoch.
Recording evidence binds the current epoch for the same reason: it is the
input completion is derived from. Reconciling work that straddles a scheduler
restart is crash-recovery, a later milestone, and until it exists the M0
behaviour is to fail closed.

Completion is also final. Re-completing with the same evidence is idempotent;
re-completing with different evidence is refused, and the schema forbids
rebinding `completed_evidence_id` once set, so which observation established
completion cannot be lost.

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
The same no-delete rule covers every durable record: `task_run`,
`worker_attempt`, `workspace`, `publish_attempt`, `evidence_observation`,
`event`, `repository_subject`, `scheduler_epoch` and `scheduler_ownership`.
Immutability that stops at `UPDATE` is not immutability — a terminal task
could be resurrected, or a repository subject re-minted under a new
identifier, by dropping the row and inserting it again.

Identity immutability covers the same set. `task_run` in particular freezes
`packet_id`, `intent`, `lane` and `repository_subject_id`: the coherence
trigger fires on INSERT only, so without it the packet a task was approved
against could be swapped afterwards, re-authorising the task under a wider
scope while the packet it was actually approved against sat marked `STALE`.
`intent` matters as much, since it selects the evidence contract at
completion.

`WorkerAttemptStatus` has the same shape of rule. Live states move forward
only, and a terminal state is final:

```text
CLAIMED -> STARTING -> RUNNING -> VERIFYING
any live -> FAILED | ABANDONED | EVIDENCE_UNKNOWN
FAILED, ABANDONED, EVIDENCE_UNKNOWN are terminal
```

Reviving a terminal attempt would re-enter the per-repository modifying slot
whenever it happened to be free and would defeat every guard written in terms
of a terminal status. A retry is a new attempt with a new fencing token, not a
resurrected old one. Relatedly, a task's `current_attempt_id` only moves
forward — to a live attempt whose fence epoch is not lower than the current
one — because completion is expressed as "the evidence came from the task's
current attempt", and a pointer that could move backwards would quietly undo
that guarantee.

`PublishStatus` has the same shape of rule, because a publication recorded
`REJECTED` that could be flipped to `APPLIED` would be usable to complete a
task:

```text
PENDING  -> APPLIED | OBSERVED | REJECTED | UNKNOWN
APPLIED  -> OBSERVED
UNKNOWN  -> APPLIED | OBSERVED | REJECTED
OBSERVED, REJECTED are terminal
```

`PENDING -> OBSERVED` is permitted because a publisher can crash after the
remote mutation lands but before recording `APPLIED`, and later reconciliation
then observes the effect directly. `UNKNOWN` is the one state still open to
resolution — it means the outcome could not be determined, not that it
succeeded, and it never completes a task.

`APPLIED -> UNKNOWN` is deliberately absent. `APPLIED` already records that
the remote mutation was made, so the step would discard information — and
since `UNKNOWN -> REJECTED` is permitted, `APPLIED -> UNKNOWN -> REJECTED`
would let a publication that actually landed end up permanently recorded as
"nothing was published". `REJECTED` is terminal and the idempotency key is
unique, so that record could never be corrected. A reconciler that cannot
confirm an applied publication leaves it `APPLIED`.

An approved packet is likewise always recorded `APPROVED` and not already
expired, since transitions run one way away from authority and packets are
undeletable: either would be a durable approval that never granted anything
and could never be corrected. A task cannot be created in any terminal state
for the same reason — it could then never admit an attempt, never change
status and never be deleted — nor against a packet that no longer grants
authority, since `packet_id` is immutable once the task exists. An attempt
cannot be created terminal either: it could never transition, nothing would
accept it, and it would already have burned a fence epoch.

A publication is also always recorded at `PENDING`. The transition rules
constrain updates, so without that a row could be inserted already `OBSERVED`
— a durable claim that the effect was independently observed, with no
lifecycle ever traversed, immediately usable to complete a task.

`TaskRunStatus` completes the set:

```text
READY          -> RUNNING | BLOCKED_DESIGN | COMPLETED | ABANDONED
RUNNING        -> READY | BLOCKED_DESIGN | COMPLETED | ABANDONED
BLOCKED_DESIGN -> READY | RUNNING | ABANDONED
COMPLETED, ABANDONED are terminal
```

A task may cycle between the live states as attempts come and go and as design
questions block and unblock it, but an explicitly withdrawn task is not
re-admitted and a completed one is not reopened. A terminal task also cannot
admit a new attempt or change its current attempt: doing so would occupy the
repository's single modifying slot on behalf of a task that is over, and would
move `current_attempt_id` away from the attempt whose evidence established
completion.

### CC9 — lane-agnostic modifying admission

```sql
CREATE UNIQUE INDEX ux_modifying_slot_per_repository
    ON worker_attempt (repository_subject_id)
    WHERE intent = 'MODIFYING'
      AND status IN ('CLAIMED', 'STARTING', 'RUNNING', 'VERIFYING');
```

The exclusion key is the repository subject, not the lane. No lane can obtain a
private modifying lock, because there is no lane column in the index.

Contention on that slot is an ordinary scheduling outcome rather than a fault,
so it surfaces as `ErrModifyingSlotBusy` — a caller needs to tell "come back
later" apart from a broken database without matching on driver strings.
Workspace collisions are typed the same way, as `ErrWorkspaceConflict`.

### CC10 — stable identifiers and event identity

One distinct Go type per identifier kind, each with a kind prefix carried in
the stored value, so a misrouted identifier is a compile error in code and
detectable in data. Event order identity is `(scheduler_epoch, seq)`, a
composite primary key, with `seq` allocated by the store inside the appending
transaction.

History reads page on `(epoch, seq)` rather than an offset, so a page boundary
cannot shift under a concurrent append: `seq` is monotonic within an epoch and
history is append-only, so the next page starts exactly where the last ended.
`EventsInEpoch` reads a whole generation and is bounded by an explicit cap —
an epoch lasts as long as a scheduler generation and the table can never be
pruned, so growth surfaces as an error naming `EventsInEpochPage` rather than
as memory pressure.

## SQLite backend

WAL, a single connection matching the single-active-scheduler model, and
`BEGIN IMMEDIATE` via `_txlock=immediate`.

The `db_contract` marker is checked on open, not merely written: an unread
marker looks like a fail-closed guard while being inert, and a future build
that bumps the contract would open an older database without noticing.

Open also refuses a database whose recorded migration history does not match
this build's — a newer version, a tampered checksum, or a migration name this
build has never heard of. `Migrate` and `VerifySchema` run the same check, but
neither is on the Open path: a read-only inspector, a backup job or an
integrity checker would otherwise read, and a read-write handle would write, a
database carrying invariants this build does not know. A partially migrated
database still opens, since that is one a process may be about to migrate
forward.

Open runs SQLite's quick integrity check rather than the full one. The full
check cross-checks every index against its table, so its cost grows with the
database — and event history is append-only with no prune path, so on a
long-serving scheduler every restart would pay a full-file scan before the
epoch could be activated. The quick check still detects a malformed page,
which is what open-time verification exists to catch; the full check stays
available as `IntegrityCheck`, for a schedule or an operator tool, and
`FullIntegrityCheckOnOpen` opts into it.

Everything that inspects the file runs before anything that writes to it.
`PRAGMA journal_mode = WAL` rewrites the database header and leaves `-wal` and
`-shm` sidecars, so setting it before the identity check would convert an
unrelated service's database on the way to refusing it — damaging the very
file the check exists to protect.

`Config.Path` is resolved to an absolute path at open time. The DSN is a
`file:` URI, and a relative path inside one is read as a URI authority rather
than a path, so a relative path would otherwise fail with an opaque driver
error after the stat and directory creation had already succeeded.

Concurrent callers are safe. The store holds a single connection, matching the
single-active-scheduler model, so transactions serialise on a mutex held for
the duration of each one and a second goroutine simply waits its turn.

Transactions do not nest, though: an inner transaction on the same goroutine
would wait for the connection the outer one holds and never get it. The two
cases need opposite answers — one should wait, the other must not — so they
are told apart by which goroutine holds the lock, and a nested call returns
`ErrNestedTransaction` immediately instead of deadlocking. A per-handle flag
would have been simpler and wrong: it rejects legitimate concurrency as if it
were nesting.

The handle-level queries — `SchemaVersion`, `VerifySchema`,
`AppliedMigrations`, `IntegrityCheck`, `VerifyConnectionPragmas`, `Migrate` —
carry the same guard. They go straight to the pool rather than through a
transaction, so without it they would reach the identical hang by a different
door.

`Read` runs a genuinely read-only transaction: the driver treats
`sql.TxOptions.ReadOnly` as a hint about which `BEGIN` to issue and enforces
nothing, so every write in the package goes through one gate that refuses to
run inside a read transaction. The gate is in Go rather than a `query_only`
pragma because that pragma is connection state, and on a single-connection
store resetting it has to happen while the transaction still holds the
connection — get the ordering wrong and it deadlocks, or worse leaves the
handle read-only permanently.

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
consistency check — including under concurrency. Two processes starting
together both see an unmigrated database, since nothing fences them before
`ActivateScheduler`, so the applying transaction re-reads the ledger inside
its own `BEGIN IMMEDIATE` and treats an already-recorded version as success.
A version recorded under a different name or checksum still fails closed:
that is divergence, not a race. A database migrated beyond what the running build knows
fails with `ErrSchemaVersionUnsupported` rather than being treated as close
enough. Each migration runs inside one transaction together with its own
ledger entry, so a step can never be recorded as applied when it was not.

Event fields are normalised through JSON before redaction so that redaction
sees the shape that will be stored, and numbers are decoded as `json.Number`
rather than `float64`. Decoding into `float64` would silently corrupt any
integer beyond 2^53 — a nanosecond timestamp, a byte count, a numeric external
id — and event history is append-only, so a number mangled on the way in could
never be corrected. Reads decode the same way, so a value read out of history
is the value that was stored.

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
