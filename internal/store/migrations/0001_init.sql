-- 0001_init: M0 durable foundation of the Agent Execution Control Plane.
--
-- Invariants that can be expressed in the schema are expressed here, so that a
-- future code path cannot bypass them by forgetting a check. Where a CHECK or
-- a partial unique index is enough, no trigger is used; triggers cover
-- cross-row identity coherence, which constraints cannot express.

-- Note: `control_plane_meta` (the database identity marker) and
-- `schema_migration` (the migration ledger) are NOT created here. They are
-- created together in one bootstrap transaction before any migration runs, so
-- that a crash between bootstrap and the first migration leaves a database
-- that is recognisably ours and safely retryable rather than one that later
-- fails as foreign. See store.bootstrap.

-- ---------------------------------------------------------------------------
-- SchedulerEpoch (CC4): scheduler ownership generation.
--
-- Rows are appended only by a successful scheduler activation transaction.
-- Opening the database, checking integrity or taking a backup must not insert
-- here; nothing in Open() writes to this table.
-- ---------------------------------------------------------------------------
CREATE TABLE scheduler_epoch (
    epoch             INTEGER PRIMARY KEY CHECK (epoch >= 1),
    owner_id          TEXT NOT NULL CHECK (length(owner_id) > 0),
    activated_at      TEXT NOT NULL CHECK (length(activated_at) > 0),
    activation_reason TEXT NOT NULL CHECK (length(activation_reason) > 0),
    released_at       TEXT
);

-- Current ownership is a singleton row. current_epoch may only move forward:
-- an older scheduler generation can never reinstate itself as the owner.
CREATE TABLE scheduler_ownership (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    current_epoch INTEGER NOT NULL REFERENCES scheduler_epoch (epoch),
    owner_id      TEXT NOT NULL CHECK (length(owner_id) > 0),
    acquired_at   TEXT NOT NULL
);

CREATE TRIGGER trg_scheduler_ownership_no_regress
BEFORE UPDATE ON scheduler_ownership
FOR EACH ROW
WHEN NEW.current_epoch <= OLD.current_epoch
BEGIN
    SELECT RAISE(ABORT, 'scheduler ownership epoch must move forward');
END;

-- Ownership history is append-only. Deleting an epoch, or the ownership row,
-- would let a retired generation reacquire ownership by re-activating at a
-- number it already used.
CREATE TRIGGER trg_scheduler_epoch_no_delete
BEFORE DELETE ON scheduler_epoch
BEGIN
    SELECT RAISE(ABORT, 'scheduler epochs are durable and cannot be deleted');
END;

CREATE TRIGGER trg_scheduler_ownership_no_delete
BEFORE DELETE ON scheduler_ownership
BEGIN
    SELECT RAISE(ABORT, 'scheduler ownership cannot be deleted');
END;

CREATE TRIGGER trg_scheduler_epoch_monotonic
BEFORE INSERT ON scheduler_epoch
FOR EACH ROW
WHEN NEW.epoch <= COALESCE((SELECT MAX(epoch) FROM scheduler_epoch), 0)
BEGIN
    SELECT RAISE(ABORT, 'scheduler epoch must be strictly increasing');
END;

-- ---------------------------------------------------------------------------
-- RepositorySubject (CC5): stable GitHub identity, mutable owner/name alias.
--
-- github_node_id is UNIQUE, so an owner/name rename observed for the same
-- node id updates the alias of the existing subject. current_full_name is
-- deliberately NOT unique: our observations lag GitHub, and a rename can
-- transiently make a name look shared. Alias lookups fail closed on ambiguity
-- instead of pretending an alias is an identity.
-- ---------------------------------------------------------------------------
CREATE TABLE repository_subject (
    repository_subject_id TEXT PRIMARY KEY,
    github_node_id        TEXT NOT NULL UNIQUE CHECK (length(github_node_id) > 0),
    current_full_name     TEXT NOT NULL CHECK (length(current_full_name) > 0),
    observed_at           TEXT NOT NULL
) WITHOUT ROWID;

CREATE INDEX ix_repository_subject_full_name
    ON repository_subject (current_full_name);

-- A subject is never deleted: dropping an unreferenced one and re-inserting
-- it would mint a new repository_subject_id for the same github_node_id,
-- which is precisely the identity fork CC5 exists to prevent.
CREATE TRIGGER trg_repository_subject_no_delete
BEFORE DELETE ON repository_subject
BEGIN
    SELECT RAISE(ABORT, 'repository subjects are durable and cannot be deleted');
END;

-- The stable identity of a subject never changes once observed.
CREATE TRIGGER trg_repository_subject_node_id_immutable
BEFORE UPDATE ON repository_subject
FOR EACH ROW
WHEN NEW.github_node_id <> OLD.github_node_id
BEGIN
    SELECT RAISE(ABORT, 'repository subject github_node_id is immutable');
END;

-- ---------------------------------------------------------------------------
-- ExecutionPacket (CC8): closed-world bounded execution authority.
-- ---------------------------------------------------------------------------
CREATE TABLE execution_packet (
    packet_id             TEXT PRIMARY KEY,
    task_id               TEXT NOT NULL,
    lane                  TEXT NOT NULL CHECK (lane IN ('operator', 'guardrail', 'review')),
    intent                TEXT NOT NULL CHECK (intent IN ('MODIFYING', 'READ_ONLY')),
    repository_subject_id TEXT NOT NULL REFERENCES repository_subject (repository_subject_id),

    source_revision       TEXT NOT NULL CHECK (length(source_revision) > 0),
    source_digest         TEXT NOT NULL CHECK (length(source_digest) = 64 AND NOT source_digest GLOB '*[^0-9a-f]*'),
    packet_digest         TEXT NOT NULL CHECK (length(packet_digest) = 64 AND NOT packet_digest GLOB '*[^0-9a-f]*'),

    approved_at           TEXT NOT NULL,
    expires_at            TEXT NOT NULL CHECK (expires_at > approved_at),

    -- JSON arrays. An empty allowed_scope is rejected: an empty closed world
    -- authorises nothing, and must never be read as "unbounded".
    allowed_scope         TEXT NOT NULL CHECK (json_valid(allowed_scope) AND json_array_length(allowed_scope) > 0),
    forbidden_scope       TEXT NOT NULL CHECK (json_valid(forbidden_scope)),
    stop_conditions       TEXT NOT NULL CHECK (json_valid(stop_conditions)),
    acceptance_contract   TEXT NOT NULL CHECK (length(acceptance_contract) > 0),

    status                TEXT NOT NULL CHECK (status IN ('APPROVED', 'STALE', 'SUPERSEDED'))
) WITHOUT ROWID;

CREATE INDEX ix_execution_packet_task ON execution_packet (task_id);

-- An approved ExecutionPacket is an immutable execution contract. Every
-- authority-bearing field is frozen at approval; only `status` may move, and
-- only along a permitted transition (see the trigger below).
--
-- This covers every column of the table except `status`. Rebinding any of
-- them after approval would silently re-authorise the packet: a widened
-- scope, a later expiry, a different lane or intent, a relaxed acceptance
-- contract or a removed stop condition are all changes of authority, not
-- bookkeeping.
CREATE TRIGGER trg_execution_packet_content_immutable
BEFORE UPDATE ON execution_packet
FOR EACH ROW
WHEN NEW.packet_id            IS NOT OLD.packet_id
  OR NEW.task_id              IS NOT OLD.task_id
  OR NEW.lane                 IS NOT OLD.lane
  OR NEW.intent               IS NOT OLD.intent
  OR NEW.repository_subject_id IS NOT OLD.repository_subject_id
  OR NEW.source_revision      IS NOT OLD.source_revision
  OR NEW.source_digest        IS NOT OLD.source_digest
  OR NEW.packet_digest        IS NOT OLD.packet_digest
  OR NEW.approved_at          IS NOT OLD.approved_at
  OR NEW.expires_at           IS NOT OLD.expires_at
  OR NEW.allowed_scope        IS NOT OLD.allowed_scope
  OR NEW.forbidden_scope      IS NOT OLD.forbidden_scope
  OR NEW.stop_conditions      IS NOT OLD.stop_conditions
  OR NEW.acceptance_contract  IS NOT OLD.acceptance_contract
BEGIN
    SELECT RAISE(ABORT, 'approved packet content is immutable; only status may change');
END;

-- Permitted status transitions only. Authority is never regained: a packet
-- that went STALE or SUPERSEDED cannot become APPROVED again, because that
-- would revive an execution authority whose binding was already found not to
-- hold.
--
--   APPROVED -> STALE
--   APPROVED -> SUPERSEDED
--   STALE    -> SUPERSEDED
CREATE TRIGGER trg_execution_packet_status_transition
BEFORE UPDATE OF status ON execution_packet
FOR EACH ROW
WHEN NEW.status IS NOT OLD.status
 AND NOT (
        (OLD.status = 'APPROVED' AND NEW.status IN ('STALE', 'SUPERSEDED'))
     OR (OLD.status = 'STALE'    AND NEW.status = 'SUPERSEDED')
 )
BEGIN
    SELECT RAISE(ABORT, 'forbidden packet status transition');
END;

-- Deletion would defeat the immutability trigger above: a packet that no task
-- references yet could be dropped and re-inserted under the same packet_id
-- with a widened scope, which is a content change by another route. An
-- approved packet is a durable record of what was authorised, so it stays.
CREATE TRIGGER trg_execution_packet_no_delete
BEFORE DELETE ON execution_packet
BEGIN
    SELECT RAISE(ABORT, 'approved packets are immutable and cannot be deleted');
END;

-- ---------------------------------------------------------------------------
-- TaskRun (CC6): the durable scheduling subject.
-- ---------------------------------------------------------------------------
CREATE TABLE task_run (
    task_id               TEXT PRIMARY KEY,
    repository_subject_id TEXT NOT NULL REFERENCES repository_subject (repository_subject_id),
    packet_id             TEXT NOT NULL REFERENCES execution_packet (packet_id),
    lane                  TEXT NOT NULL CHECK (lane IN ('operator', 'guardrail', 'review')),
    intent                TEXT NOT NULL CHECK (intent IN ('MODIFYING', 'READ_ONLY')),

    -- TaskRunStatus only. PACKET_STALE belongs to PacketStatus and the worker
    -- execution states belong to WorkerAttemptStatus.
    status                TEXT NOT NULL CHECK (status IN ('READY', 'RUNNING', 'BLOCKED_DESIGN', 'COMPLETED', 'ABANDONED')),

    current_attempt_id    TEXT REFERENCES worker_attempt (attempt_id),
    completed_evidence_id TEXT REFERENCES evidence_observation (evidence_id),

    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL,

    -- I1: worker exit is not completion. COMPLETED cannot exist without a
    -- bound evidence observation, and evidence cannot exist without matching
    -- attempt identity (see evidence_observation triggers).
    CHECK ((status = 'COMPLETED') = (completed_evidence_id IS NOT NULL))
) WITHOUT ROWID;

CREATE INDEX ix_task_run_repository ON task_run (repository_subject_id);
CREATE INDEX ix_task_run_status ON task_run (status);

CREATE TRIGGER trg_task_run_packet_coherence
BEFORE INSERT ON task_run
FOR EACH ROW
WHEN (SELECT task_id FROM execution_packet WHERE packet_id = NEW.packet_id) IS NOT NEW.task_id
  OR (SELECT repository_subject_id FROM execution_packet WHERE packet_id = NEW.packet_id) IS NOT NEW.repository_subject_id
  OR (SELECT lane FROM execution_packet WHERE packet_id = NEW.packet_id) IS NOT NEW.lane
  OR (SELECT intent FROM execution_packet WHERE packet_id = NEW.packet_id) IS NOT NEW.intent
BEGIN
    SELECT RAISE(ABORT, 'task_run identity must match its execution_packet');
END;

-- Completion evidence must be evidence about this very task.
--
-- Guarded on INSERT as well as UPDATE. SQLite has no BEFORE INSERT OR UPDATE,
-- so each rule is written twice. The INSERT form is not redundant: today
-- CreateTaskRun hardcodes NULL for both columns, but nothing in the schema
-- would stop a future code path from inserting a task that is already
-- COMPLETED against another task's evidence, or that points at another
-- task's attempt. Both foreign keys would resolve; only ownership would be
-- wrong, and this file's premise is that the schema does not rely on code
-- remembering to check.
CREATE TRIGGER trg_task_run_completion_evidence_binding_ins
BEFORE INSERT ON task_run
FOR EACH ROW
WHEN NEW.completed_evidence_id IS NOT NULL
  AND (SELECT task_id FROM evidence_observation WHERE evidence_id = NEW.completed_evidence_id) IS NOT NEW.task_id
BEGIN
    SELECT RAISE(ABORT, 'completion evidence must be attributed to this task');
END;

CREATE TRIGGER trg_task_run_completion_evidence_binding
BEFORE UPDATE ON task_run
FOR EACH ROW
WHEN NEW.completed_evidence_id IS NOT NULL
  AND (SELECT task_id FROM evidence_observation WHERE evidence_id = NEW.completed_evidence_id) IS NOT NEW.task_id
BEGIN
    SELECT RAISE(ABORT, 'completion evidence must be attributed to this task');
END;

-- A task's identity is fixed at creation. task_run was the only durable
-- entity without this, and the coherence trigger above fires on INSERT only,
-- so the packet a task was approved against could be swapped afterwards —
-- re-authorising the task under a different, possibly wider scope while the
-- packet it was actually approved against sat marked STALE. `intent` matters
-- just as much: it is what selects the evidence contract at completion.
--
-- Only status, current_attempt_id, completed_evidence_id and updated_at move.
CREATE TRIGGER trg_task_run_identity_immutable
BEFORE UPDATE ON task_run
FOR EACH ROW
WHEN NEW.task_id               IS NOT OLD.task_id
  OR NEW.repository_subject_id IS NOT OLD.repository_subject_id
  OR NEW.packet_id             IS NOT OLD.packet_id
  OR NEW.lane                  IS NOT OLD.lane
  OR NEW.intent                IS NOT OLD.intent
  OR NEW.created_at            IS NOT OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'task_run identity is immutable');
END;

-- Durable execution records are never deleted. Without this a terminal task
-- could be resurrected by DELETE followed by INSERT with the same task_id,
-- defeating the status-transition rule by the same route the packet no-delete
-- trigger exists to block.
CREATE TRIGGER trg_task_run_no_delete
BEFORE DELETE ON task_run
BEGIN
    SELECT RAISE(ABORT, 'task runs are durable and cannot be deleted');
END;

-- Permitted task status transitions only. READY, RUNNING and BLOCKED_DESIGN
-- may cycle as attempts come and go and as design questions block and unblock
-- the task; COMPLETED and ABANDONED are terminal. An explicitly withdrawn
-- task must not be silently re-admitted for execution, and a completed one
-- must not be reopened.
CREATE TRIGGER trg_task_run_status_transition
BEFORE UPDATE OF status ON task_run
FOR EACH ROW
WHEN NEW.status IS NOT OLD.status
 AND NOT (
        (OLD.status = 'READY'          AND NEW.status IN ('RUNNING', 'BLOCKED_DESIGN', 'COMPLETED', 'ABANDONED'))
     OR (OLD.status = 'RUNNING'        AND NEW.status IN ('READY', 'BLOCKED_DESIGN', 'COMPLETED', 'ABANDONED'))
     OR (OLD.status = 'BLOCKED_DESIGN' AND NEW.status IN ('READY', 'RUNNING', 'ABANDONED'))
 )
BEGIN
    SELECT RAISE(ABORT, 'forbidden task run status transition');
END;

-- A task may only point at an attempt that belongs to it.
CREATE TRIGGER trg_task_run_current_attempt_binding_ins
BEFORE INSERT ON task_run
FOR EACH ROW
WHEN NEW.current_attempt_id IS NOT NULL
  AND (SELECT task_id FROM worker_attempt WHERE attempt_id = NEW.current_attempt_id) IS NOT NEW.task_id
BEGIN
    SELECT RAISE(ABORT, 'current_attempt_id must belong to this task');
END;

CREATE TRIGGER trg_task_run_current_attempt_binding
BEFORE UPDATE ON task_run
FOR EACH ROW
WHEN NEW.current_attempt_id IS NOT NULL
  AND (SELECT task_id FROM worker_attempt WHERE attempt_id = NEW.current_attempt_id) IS NOT NEW.task_id
BEGIN
    SELECT RAISE(ABORT, 'current_attempt_id must belong to this task');
END;

-- The pointer only moves forward, and only onto a live attempt. Completion is
-- expressed as "the evidence came from the task's current attempt", so a
-- pointer that could be moved backwards onto a fenced-out attempt would
-- quietly undo that guarantee.
CREATE TRIGGER trg_task_run_current_attempt_forward
BEFORE UPDATE OF current_attempt_id ON task_run
FOR EACH ROW
WHEN NEW.current_attempt_id IS NOT NULL
  AND NEW.current_attempt_id IS NOT OLD.current_attempt_id
  AND (
        (SELECT status FROM worker_attempt WHERE attempt_id = NEW.current_attempt_id)
            IN ('FAILED', 'ABANDONED', 'EVIDENCE_UNKNOWN')
     OR (OLD.current_attempt_id IS NOT NULL
         AND (SELECT fence_epoch FROM worker_attempt WHERE attempt_id = NEW.current_attempt_id)
             < (SELECT fence_epoch FROM worker_attempt WHERE attempt_id = OLD.current_attempt_id))
  )
BEGIN
    SELECT RAISE(ABORT, 'current_attempt_id must move forward to a live attempt');
END;

-- The attempt a task points at must belong to the current generation.
-- Pointing at a retired generation's attempt wedges the task: it holds the
-- repository's modifying slot so no replacement can be admitted, while
-- evidence against it is refused for the stale epoch.
CREATE TRIGGER trg_task_run_current_attempt_epoch_current
BEFORE UPDATE OF current_attempt_id ON task_run
FOR EACH ROW
WHEN NEW.current_attempt_id IS NOT NULL
  AND NEW.current_attempt_id IS NOT OLD.current_attempt_id
  AND (SELECT scheduler_epoch FROM worker_attempt WHERE attempt_id = NEW.current_attempt_id)
      IS NOT (SELECT current_epoch FROM scheduler_ownership WHERE id = 1)
BEGIN
    SELECT RAISE(ABORT, 'current_attempt_id must bind the current scheduler epoch');
END;

-- Once a task is finished its current attempt is settled. Moving the pointer
-- afterwards would separate the task from the attempt whose evidence
-- established completion.
CREATE TRIGGER trg_task_run_current_attempt_frozen_when_terminal
BEFORE UPDATE OF current_attempt_id ON task_run
FOR EACH ROW
WHEN OLD.status IN ('COMPLETED', 'ABANDONED')
 AND NEW.current_attempt_id IS NOT OLD.current_attempt_id
BEGIN
    SELECT RAISE(ABORT, 'a terminal task cannot change its current attempt');
END;

-- #5: completion evidence, once bound, is the observation that established
-- completion. Rebinding it would lose which observation actually did.
CREATE TRIGGER trg_task_run_completion_evidence_final
BEFORE UPDATE OF completed_evidence_id ON task_run
FOR EACH ROW
WHEN OLD.completed_evidence_id IS NOT NULL
 AND NEW.completed_evidence_id IS NOT OLD.completed_evidence_id
BEGIN
    SELECT RAISE(ABORT, 'completion evidence is final once bound');
END;

-- ---------------------------------------------------------------------------
-- WorkerAttempt: one bounded execution, fenced within its task and bound to
-- the scheduler generation that admitted it.
-- ---------------------------------------------------------------------------
CREATE TABLE worker_attempt (
    attempt_id            TEXT PRIMARY KEY,
    task_id               TEXT NOT NULL REFERENCES task_run (task_id),
    packet_id             TEXT NOT NULL REFERENCES execution_packet (packet_id),
    repository_subject_id TEXT NOT NULL REFERENCES repository_subject (repository_subject_id),
    lane                  TEXT NOT NULL CHECK (lane IN ('operator', 'guardrail', 'review')),
    intent                TEXT NOT NULL CHECK (intent IN ('MODIFYING', 'READ_ONLY')),

    scheduler_epoch       INTEGER NOT NULL REFERENCES scheduler_epoch (epoch),
    fence_epoch           INTEGER NOT NULL CHECK (fence_epoch >= 1),

    -- WorkerAttemptStatus only. COMPLETED is absent by design: completion is
    -- a property of a TaskRun derived from evidence, not of a process.
    status                TEXT NOT NULL CHECK (status IN ('CLAIMED', 'STARTING', 'RUNNING', 'VERIFYING', 'FAILED', 'ABANDONED', 'EVIDENCE_UNKNOWN')),

    lease_expires_at      TEXT,
    last_checkpoint_at    TEXT,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL,

    UNIQUE (task_id, fence_epoch)
) WITHOUT ROWID;

CREATE INDEX ix_worker_attempt_task ON worker_attempt (task_id);
CREATE INDEX ix_worker_attempt_epoch ON worker_attempt (scheduler_epoch);

-- CC9: modifying admission is lane-agnostic. At most one live modifying
-- attempt may exist per RepositorySubject, regardless of lane. Enforced as a
-- partial unique index so no code path can grant a lane its own private lock.
CREATE UNIQUE INDEX ux_modifying_slot_per_repository
    ON worker_attempt (repository_subject_id)
    WHERE intent = 'MODIFYING'
      AND status IN ('CLAIMED', 'STARTING', 'RUNNING', 'VERIFYING');

-- CC4: an attempt may only be admitted under the current scheduler generation.
-- An old-epoch record stays readable but can never acquire ownership.
CREATE TRIGGER trg_worker_attempt_epoch_current
BEFORE INSERT ON worker_attempt
FOR EACH ROW
WHEN NEW.scheduler_epoch IS NOT (SELECT current_epoch FROM scheduler_ownership WHERE id = 1)
BEGIN
    SELECT RAISE(ABORT, 'worker_attempt must bind the current scheduler epoch');
END;

CREATE TRIGGER trg_worker_attempt_task_coherence
BEFORE INSERT ON worker_attempt
FOR EACH ROW
WHEN (SELECT repository_subject_id FROM task_run WHERE task_id = NEW.task_id) IS NOT NEW.repository_subject_id
  OR (SELECT packet_id FROM task_run WHERE task_id = NEW.task_id) IS NOT NEW.packet_id
  OR (SELECT lane FROM task_run WHERE task_id = NEW.task_id) IS NOT NEW.lane
  OR (SELECT intent FROM task_run WHERE task_id = NEW.task_id) IS NOT NEW.intent
BEGIN
    SELECT RAISE(ABORT, 'worker_attempt identity must match its task_run');
END;
-- An attempt may only be admitted under a packet that still grants execution
-- authority. A STALE or SUPERSEDED packet means execution must stop, so it
-- must not be the basis for starting more of it. Expiry is checked in Go,
-- since the stored timestamps are not in a format SQLite can compare against
-- its own clock.
CREATE TRIGGER trg_worker_attempt_packet_authority
BEFORE INSERT ON worker_attempt
FOR EACH ROW
WHEN (SELECT status FROM execution_packet WHERE packet_id = NEW.packet_id) IS NOT 'APPROVED'
BEGIN
    SELECT RAISE(ABORT, 'an attempt requires a packet that grants execution authority');
END;

-- A finished task does not take on new work: a new attempt would occupy the
-- repository's single modifying slot on behalf of a task that is over.
CREATE TRIGGER trg_worker_attempt_task_not_terminal
BEFORE INSERT ON worker_attempt
FOR EACH ROW
WHEN (SELECT status FROM task_run WHERE task_id = NEW.task_id) IN ('COMPLETED', 'ABANDONED')
BEGIN
    SELECT RAISE(ABORT, 'a terminal task cannot admit a new attempt');
END;


CREATE TRIGGER trg_worker_attempt_fence_monotonic
BEFORE INSERT ON worker_attempt
FOR EACH ROW
WHEN NEW.fence_epoch <= COALESCE((SELECT MAX(fence_epoch) FROM worker_attempt WHERE task_id = NEW.task_id), 0)
BEGIN
    SELECT RAISE(ABORT, 'fence_epoch must be monotonic within a task');
END;

CREATE TRIGGER trg_worker_attempt_identity_immutable
BEFORE UPDATE ON worker_attempt
FOR EACH ROW
WHEN NEW.attempt_id            IS NOT OLD.attempt_id
  OR NEW.task_id               IS NOT OLD.task_id
  OR NEW.packet_id             IS NOT OLD.packet_id
  OR NEW.repository_subject_id IS NOT OLD.repository_subject_id
  OR NEW.scheduler_epoch       IS NOT OLD.scheduler_epoch
  OR NEW.fence_epoch           IS NOT OLD.fence_epoch
  OR NEW.intent                IS NOT OLD.intent
  -- lane included: the coherence trigger is INSERT-only, so without this an
  -- attempt's lane could drift from its task's after admission.
  OR NEW.lane                  IS NOT OLD.lane
  OR NEW.created_at            IS NOT OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'worker_attempt identity is immutable');
END;

-- Permitted attempt status transitions only: live states move forward, and a
-- terminal state is final. Reviving a fenced-out, failed or evidence-unknown
-- attempt would re-enter the per-repository modifying slot whenever it
-- happened to be free, and would defeat every guard written in terms of a
-- terminal status. A retry is a new attempt with a new fencing token.
CREATE TRIGGER trg_worker_attempt_status_transition
BEFORE UPDATE OF status ON worker_attempt
FOR EACH ROW
WHEN NEW.status IS NOT OLD.status
 AND NOT (
        (OLD.status = 'CLAIMED'   AND NEW.status IN ('STARTING', 'RUNNING', 'VERIFYING', 'FAILED', 'ABANDONED', 'EVIDENCE_UNKNOWN'))
     OR (OLD.status = 'STARTING'  AND NEW.status IN ('RUNNING', 'VERIFYING', 'FAILED', 'ABANDONED', 'EVIDENCE_UNKNOWN'))
     OR (OLD.status = 'RUNNING'   AND NEW.status IN ('VERIFYING', 'FAILED', 'ABANDONED', 'EVIDENCE_UNKNOWN'))
     OR (OLD.status = 'VERIFYING' AND NEW.status IN ('FAILED', 'ABANDONED', 'EVIDENCE_UNKNOWN'))
 )
BEGIN
    SELECT RAISE(ABORT, 'forbidden worker attempt status transition');
END;

-- Durable execution records are never deleted. Without this, a row could be
-- dropped and re-inserted under the same identity with different content,
-- which is a content change by another route.
CREATE TRIGGER trg_worker_attempt_no_delete
BEFORE DELETE ON worker_attempt
BEGIN
    SELECT RAISE(ABORT, 'worker attempts are durable and cannot be deleted');
END;

-- ---------------------------------------------------------------------------
-- Workspace (CC3): owned by exactly one attempt, never shared, never reused.
-- ---------------------------------------------------------------------------
CREATE TABLE workspace (
    workspace_id          TEXT PRIMARY KEY,
    attempt_id            TEXT NOT NULL UNIQUE REFERENCES worker_attempt (attempt_id),
    task_id               TEXT NOT NULL REFERENCES task_run (task_id),
    repository_subject_id TEXT NOT NULL REFERENCES repository_subject (repository_subject_id),
    base_sha              TEXT NOT NULL CHECK (length(base_sha) = 40 AND NOT base_sha GLOB '*[^0-9a-f]*'),

    -- A shared Git worktree is not an isolation primitive here: worktrees
    -- share common Git metadata, and the worker can run arbitrary local Git.
    isolation_kind        TEXT NOT NULL CHECK (isolation_kind IN ('ISOLATED_CLONE', 'READ_ONLY_CHECKOUT')),

    -- UNIQUE root_path is the filesystem half of the isolation invariant: two
    -- attempts cannot be pointed at the same directory.
    root_path             TEXT NOT NULL UNIQUE CHECK (length(root_path) > 0),

    created_at            TEXT NOT NULL,
    released_at           TEXT
) WITHOUT ROWID;

CREATE TRIGGER trg_workspace_attempt_coherence
BEFORE INSERT ON workspace
FOR EACH ROW
WHEN (SELECT task_id FROM worker_attempt WHERE attempt_id = NEW.attempt_id) IS NOT NEW.task_id
  OR (SELECT repository_subject_id FROM worker_attempt WHERE attempt_id = NEW.attempt_id) IS NOT NEW.repository_subject_id
BEGIN
    SELECT RAISE(ABORT, 'workspace identity must match its owning attempt');
END;

-- A finished task takes on no new durable state. The Go store enforces this,
-- but so must the schema: this file's premise is that a future code path
-- cannot bypass an invariant by forgetting a check, and withdrawal leaves the
-- attempt live, so nothing else downstream notices.
CREATE TRIGGER trg_workspace_task_not_terminal
BEFORE INSERT ON workspace
FOR EACH ROW
WHEN (SELECT status FROM task_run WHERE task_id = NEW.task_id) IN ('COMPLETED', 'ABANDONED')
BEGIN
    SELECT RAISE(ABORT, 'a terminal task cannot take a new workspace');
END;

-- CC3's rule is a fresh isolated clone per modifying attempt, so the
-- isolation kind is determined by the attempt's intent rather than chosen
-- freely. Without this a MODIFYING attempt could own a READ_ONLY_CHECKOUT and
-- still publish from it: the publish coherence trigger checks the attempt's
-- intent, the workspace's owner and its base SHA, but not its isolation.
CREATE TRIGGER trg_workspace_isolation_matches_intent
BEFORE INSERT ON workspace
FOR EACH ROW
WHEN NEW.isolation_kind IS NOT (
        CASE (SELECT intent FROM worker_attempt WHERE attempt_id = NEW.attempt_id)
            WHEN 'MODIFYING' THEN 'ISOLATED_CLONE'
            WHEN 'READ_ONLY' THEN 'READ_ONLY_CHECKOUT'
        END)
BEGIN
    SELECT RAISE(ABORT, 'workspace isolation_kind must match the attempt intent');
END;

CREATE TRIGGER trg_workspace_ownership_immutable
BEFORE UPDATE ON workspace
FOR EACH ROW
WHEN NEW.workspace_id          IS NOT OLD.workspace_id
  OR NEW.attempt_id            IS NOT OLD.attempt_id
  OR NEW.task_id               IS NOT OLD.task_id
  OR NEW.repository_subject_id IS NOT OLD.repository_subject_id
  OR NEW.base_sha              IS NOT OLD.base_sha
  OR NEW.root_path             IS NOT OLD.root_path
  -- isolation_kind included: the coherence trigger is INSERT-only, so
  -- without this a READ_ONLY_CHECKOUT could be relabelled an ISOLATED_CLONE
  -- after the fact. Only released_at is meant to change.
  OR NEW.isolation_kind        IS NOT OLD.isolation_kind
  OR NEW.created_at            IS NOT OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'workspace ownership is immutable; a workspace cannot be rebound to another attempt');
END;

-- released_at is the one mutable column, and it only moves forward: from NULL
-- to a timestamp, once. Resetting it to NULL would make a released workspace
-- read as live again, defeating both released-workspace guards; overwriting
-- it would lose when the workspace actually stopped being usable.
CREATE TRIGGER trg_workspace_release_is_final
BEFORE UPDATE OF released_at ON workspace
FOR EACH ROW
WHEN OLD.released_at IS NOT NULL
 AND NEW.released_at IS NOT OLD.released_at
BEGIN
    SELECT RAISE(ABORT, 'workspace release is final');
END;

CREATE TRIGGER trg_workspace_no_delete
BEFORE DELETE ON workspace
BEGIN
    SELECT RAISE(ABORT, 'workspaces are durable and cannot be deleted');
END;

-- ---------------------------------------------------------------------------
-- PublishAttempt (CC1): exact immutable commit binding.
--
-- M0 stores the identity. No push, no GitHub mutation and no credential
-- handling exists in this milestone.
-- ---------------------------------------------------------------------------
CREATE TABLE publish_attempt (
    publish_attempt_id    TEXT PRIMARY KEY,
    task_id               TEXT NOT NULL REFERENCES task_run (task_id),
    attempt_id            TEXT NOT NULL REFERENCES worker_attempt (attempt_id),
    scheduler_epoch       INTEGER NOT NULL REFERENCES scheduler_epoch (epoch),
    fence_epoch           INTEGER NOT NULL CHECK (fence_epoch >= 1),
    workspace_id          TEXT NOT NULL REFERENCES workspace (workspace_id),
    repository_subject_id TEXT NOT NULL REFERENCES repository_subject (repository_subject_id),

    base_sha              TEXT NOT NULL CHECK (length(base_sha) = 40 AND NOT base_sha GLOB '*[^0-9a-f]*'),

    -- "publish branch HEAD" is forbidden; an exact immutable commit is required.
    source_commit_sha     TEXT NOT NULL CHECK (length(source_commit_sha) = 40 AND NOT source_commit_sha GLOB '*[^0-9a-f]*'),
    -- GLOB rather than LIKE for the symbolic-ref check: SQLite's LIKE is
    -- case-insensitive, which would reject every refs/heads/... target. Only
    -- a whole trailing HEAD component is symbolic — a branch such as
    -- refs/heads/fix-HEADER-parsing is an ordinary ref, and a substring match
    -- would make it permanently unpublishable. The per-component rules of
    -- git check-ref-format live in domain.ValidateTargetRef; the column keeps
    -- the load-bearing part.
    target_ref            TEXT NOT NULL CHECK (target_ref GLOB 'refs/*' AND target_ref NOT GLOB '*/HEAD' AND target_ref NOT LIKE '% %' AND target_ref NOT LIKE '%*%' AND target_ref NOT LIKE '%..%'),

    -- A retry of the same intent reproduces the same key and is rejected as a
    -- duplicate publication identity.
    idempotency_key       TEXT NOT NULL UNIQUE CHECK (length(idempotency_key) > 0),

    status                TEXT NOT NULL CHECK (status IN ('PENDING', 'APPLIED', 'OBSERVED', 'REJECTED', 'UNKNOWN')),
    created_at            TEXT NOT NULL,
    -- When the status last moved. task_run carries updated_at and workspace
    -- carries released_at; without this the single most irreversible entity
    -- would be the one recording nothing about when it changed, which is
    -- exactly what a publisher resuming after a crash needs to know.
    updated_at            TEXT NOT NULL
) WITHOUT ROWID;

CREATE INDEX ix_publish_attempt_attempt ON publish_attempt (attempt_id);

CREATE TRIGGER trg_publish_attempt_epoch_current
BEFORE INSERT ON publish_attempt
FOR EACH ROW
WHEN NEW.scheduler_epoch IS NOT (SELECT current_epoch FROM scheduler_ownership WHERE id = 1)
BEGIN
    SELECT RAISE(ABORT, 'publish_attempt must bind the current scheduler epoch');
END;

-- Publication is the irreversible step, so the task-liveness fence matters
-- most here: a cancelled task whose push still went out is the worst outcome
-- this control plane can produce.
CREATE TRIGGER trg_publish_attempt_task_not_terminal
BEFORE INSERT ON publish_attempt
FOR EACH ROW
WHEN (SELECT status FROM task_run WHERE task_id = NEW.task_id) IN ('COMPLETED', 'ABANDONED')
BEGIN
    SELECT RAISE(ABORT, 'a terminal task cannot publish');
END;

-- Every identity field must agree with the attempt and the workspace. A stale
-- or foreign attempt therefore cannot borrow another attempt's publication
-- identity or another workspace's commit metadata.
CREATE TRIGGER trg_publish_attempt_binding_coherence
BEFORE INSERT ON publish_attempt
FOR EACH ROW
WHEN (SELECT task_id FROM worker_attempt WHERE attempt_id = NEW.attempt_id) IS NOT NEW.task_id
  OR (SELECT scheduler_epoch FROM worker_attempt WHERE attempt_id = NEW.attempt_id) IS NOT NEW.scheduler_epoch
  OR (SELECT fence_epoch FROM worker_attempt WHERE attempt_id = NEW.attempt_id) IS NOT NEW.fence_epoch
  OR (SELECT repository_subject_id FROM worker_attempt WHERE attempt_id = NEW.attempt_id) IS NOT NEW.repository_subject_id
  OR (SELECT intent FROM worker_attempt WHERE attempt_id = NEW.attempt_id) IS NOT 'MODIFYING'
  OR (SELECT attempt_id FROM workspace WHERE workspace_id = NEW.workspace_id) IS NOT NEW.attempt_id
  OR (SELECT base_sha FROM workspace WHERE workspace_id = NEW.workspace_id) IS NOT NEW.base_sha
BEGIN
    SELECT RAISE(ABORT, 'publish_attempt binding must match its attempt and workspace');
END;

-- Permitted publication status transitions only. A REJECTED or OBSERVED
-- outcome is final, and nothing returns to PENDING, so a publication recorded
-- as rejected cannot be flipped to APPLIED and then used to complete a task.
--
-- PENDING   -> APPLIED | OBSERVED | REJECTED | UNKNOWN
-- APPLIED   -> OBSERVED
-- UNKNOWN   -> APPLIED | OBSERVED | REJECTED
-- OBSERVED, REJECTED are terminal.
--
-- PENDING -> OBSERVED is permitted because a publisher can crash after the
-- remote mutation lands but before recording APPLIED; later reconciliation
-- then observes the effect directly.
CREATE TRIGGER trg_publish_attempt_status_transition
BEFORE UPDATE OF status ON publish_attempt
FOR EACH ROW
WHEN NEW.status IS NOT OLD.status
 AND NOT (
        (OLD.status = 'PENDING' AND NEW.status IN ('APPLIED', 'OBSERVED', 'REJECTED', 'UNKNOWN'))
     -- Deliberately not APPLIED -> UNKNOWN: combined with
     -- UNKNOWN -> REJECTED it would let a landed publication be recorded
     -- permanently as "nothing was published", uncorrectably.
     OR (OLD.status = 'APPLIED' AND NEW.status = 'OBSERVED')
     OR (OLD.status = 'UNKNOWN' AND NEW.status IN ('APPLIED', 'OBSERVED', 'REJECTED'))
 )
BEGIN
    SELECT RAISE(ABORT, 'forbidden publish status transition');
END;

-- A released workspace is not publishable. The Go store enforces this; so
-- must the schema, on the same premise as the fences around it.
CREATE TRIGGER trg_publish_attempt_workspace_live
BEFORE INSERT ON publish_attempt
FOR EACH ROW
WHEN (SELECT released_at FROM workspace WHERE workspace_id = NEW.workspace_id) IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'a released workspace cannot publish');
END;

CREATE TRIGGER trg_publish_attempt_identity_immutable
BEFORE UPDATE ON publish_attempt
FOR EACH ROW
WHEN NEW.publish_attempt_id    <> OLD.publish_attempt_id
  OR NEW.task_id               <> OLD.task_id
  OR NEW.attempt_id            <> OLD.attempt_id
  OR NEW.scheduler_epoch       <> OLD.scheduler_epoch
  OR NEW.fence_epoch           <> OLD.fence_epoch
  OR NEW.workspace_id          <> OLD.workspace_id
  OR NEW.repository_subject_id <> OLD.repository_subject_id
  OR NEW.base_sha              <> OLD.base_sha
  OR NEW.source_commit_sha     <> OLD.source_commit_sha
  OR NEW.target_ref            <> OLD.target_ref
  OR NEW.idempotency_key       IS NOT OLD.idempotency_key
  OR NEW.created_at            IS NOT OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'publish_attempt identity is immutable; only status and updated_at may change');
END;

CREATE TRIGGER trg_publish_attempt_no_delete
BEFORE DELETE ON publish_attempt
BEGIN
    SELECT RAISE(ABORT, 'publication records are durable and cannot be deleted');
END;

-- ---------------------------------------------------------------------------
-- EvidenceObservation (CC7): exact-attempt, exact-published-SHA identity.
-- ---------------------------------------------------------------------------
CREATE TABLE evidence_observation (
    evidence_id           TEXT PRIMARY KEY,
    task_id               TEXT NOT NULL REFERENCES task_run (task_id),
    attempt_id            TEXT NOT NULL REFERENCES worker_attempt (attempt_id),
    scheduler_epoch       INTEGER NOT NULL REFERENCES scheduler_epoch (epoch),
    fence_epoch           INTEGER NOT NULL CHECK (fence_epoch >= 1),
    repository_subject_id TEXT NOT NULL REFERENCES repository_subject (repository_subject_id),
    workspace_id          TEXT NOT NULL REFERENCES workspace (workspace_id),
    publish_attempt_id    TEXT REFERENCES publish_attempt (publish_attempt_id),

    published_sha         TEXT NOT NULL CHECK (length(published_sha) = 40 AND NOT published_sha GLOB '*[^0-9a-f]*'),
    observed_sha          TEXT NOT NULL CHECK (length(observed_sha) = 40 AND NOT observed_sha GLOB '*[^0-9a-f]*'),

    -- Read-only review binding (CC7). NULL for repository-effect evidence.
    reviewed_sha          TEXT CHECK (reviewed_sha IS NULL OR (length(reviewed_sha) = 40 AND NOT reviewed_sha GLOB '*[^0-9a-f]*')),
    artifact_digest       TEXT CHECK (artifact_digest IS NULL OR (length(artifact_digest) = 64 AND NOT artifact_digest GLOB '*[^0-9a-f]*')),

    observed_at           TEXT NOT NULL,
    evidence_kind         TEXT NOT NULL CHECK (evidence_kind IN ('BRANCH_HEAD', 'PULL_REQUEST_HEAD', 'READ_ONLY_REVIEW')),

    -- CC7: read-only review evidence must bind the fixed reviewed SHA and the
    -- immutable review artifact digest, and repository-effect evidence must
    -- not carry either of them.
    --
    -- One biconditional per column, deliberately. A single combined
    -- `(kind = 'READ_ONLY_REVIEW') = (reviewed_sha IS NOT NULL AND
    -- artifact_digest IS NOT NULL)` is satisfied by a BRANCH_HEAD row that
    -- carries exactly one of the two, because the conjunction is then false
    -- on both sides. Per-column keeps the two kinds of evidence from blurring
    -- into each other in either direction.
    CHECK ((evidence_kind = 'READ_ONLY_REVIEW') = (reviewed_sha IS NOT NULL)),
    CHECK ((evidence_kind = 'READ_ONLY_REVIEW') = (artifact_digest IS NOT NULL)),

    -- The review is bound to the exact commit it reviewed. Without this a
    -- review could name one commit and be observed against another.
    CHECK (evidence_kind <> 'READ_ONLY_REVIEW'
           OR (reviewed_sha = observed_sha AND reviewed_sha = published_sha)),

    -- A read-only review publishes nothing, so it can never borrow a
    -- publication to look like a repository effect.
    CHECK (evidence_kind <> 'READ_ONLY_REVIEW' OR publish_attempt_id IS NULL)
) WITHOUT ROWID;

CREATE INDEX ix_evidence_attempt ON evidence_observation (attempt_id);

-- An observation filed against a finished task is permanent state belonging
-- to work that is over: immutable, undeletable, and rejected by completion.
CREATE TRIGGER trg_evidence_task_not_terminal
BEFORE INSERT ON evidence_observation
FOR EACH ROW
WHEN (SELECT status FROM task_run WHERE task_id = NEW.task_id) IN ('COMPLETED', 'ABANDONED')
BEGIN
    SELECT RAISE(ABORT, 'a terminal task cannot record evidence');
END;

-- Evidence must be about the attempt it names, in every identity dimension.
CREATE TRIGGER trg_evidence_attribution_coherence
BEFORE INSERT ON evidence_observation
FOR EACH ROW
WHEN (SELECT task_id FROM worker_attempt WHERE attempt_id = NEW.attempt_id) IS NOT NEW.task_id
  OR (SELECT scheduler_epoch FROM worker_attempt WHERE attempt_id = NEW.attempt_id) IS NOT NEW.scheduler_epoch
  OR (SELECT fence_epoch FROM worker_attempt WHERE attempt_id = NEW.attempt_id) IS NOT NEW.fence_epoch
  OR (SELECT repository_subject_id FROM worker_attempt WHERE attempt_id = NEW.attempt_id) IS NOT NEW.repository_subject_id
  OR (SELECT attempt_id FROM workspace WHERE workspace_id = NEW.workspace_id) IS NOT NEW.attempt_id
BEGIN
    SELECT RAISE(ABORT, 'evidence_observation must be attributed to its own attempt identity');
END;

-- If evidence names a publication, it must be that attempt's publication and
-- must carry that publication's exact commit.
CREATE TRIGGER trg_evidence_publish_coherence
BEFORE INSERT ON evidence_observation
FOR EACH ROW
WHEN NEW.publish_attempt_id IS NOT NULL
  AND ((SELECT attempt_id FROM publish_attempt WHERE publish_attempt_id = NEW.publish_attempt_id) IS NOT NEW.attempt_id
    OR (SELECT source_commit_sha FROM publish_attempt WHERE publish_attempt_id = NEW.publish_attempt_id) IS NOT NEW.published_sha)
BEGIN
    SELECT RAISE(ABORT, 'evidence published_sha must equal its publish_attempt source_commit_sha');
END;

CREATE TRIGGER trg_evidence_workspace_live
BEFORE INSERT ON evidence_observation
FOR EACH ROW
WHEN (SELECT released_at FROM workspace WHERE workspace_id = NEW.workspace_id) IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'a released workspace cannot record evidence');
END;

CREATE TRIGGER trg_evidence_immutable
BEFORE UPDATE ON evidence_observation
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'evidence observations are immutable');
END;

-- Immutable means undeletable too: an un-referenced observation could
-- otherwise be dropped and re-inserted under the same evidence_id with
-- different content, which is what makes the artifact digest a binding rather
-- than a note.
CREATE TRIGGER trg_evidence_no_delete
BEFORE DELETE ON evidence_observation
BEGIN
    SELECT RAISE(ABORT, 'evidence observations are immutable and cannot be deleted');
END;

-- ---------------------------------------------------------------------------
-- Event (CC10): append-only history keyed by (scheduler_epoch, seq).
-- ---------------------------------------------------------------------------
CREATE TABLE event (
    scheduler_epoch INTEGER NOT NULL REFERENCES scheduler_epoch (epoch),
    seq             INTEGER NOT NULL CHECK (seq >= 1),
    event_id        TEXT NOT NULL UNIQUE,
    occurred_at     TEXT NOT NULL,
    event_type      TEXT NOT NULL CHECK (length(event_type) > 0),
    subject_kind    TEXT NOT NULL CHECK (subject_kind IN ('SCHEDULER', 'REPOSITORY_SUBJECT', 'PACKET', 'TASK_RUN', 'WORKER_ATTEMPT', 'WORKSPACE', 'PUBLISH_ATTEMPT', 'EVIDENCE')),
    subject_id      TEXT NOT NULL CHECK (length(subject_id) > 0),
    -- Foreign keys, like every other identity relation here. AppendEvent
    -- validates only the shape of these identifiers, so without the
    -- references a wrong pointer would write a permanently misattributed
    -- event into a table that rejects UPDATE and DELETE.
    task_id         TEXT REFERENCES task_run (task_id),
    attempt_id      TEXT REFERENCES worker_attempt (attempt_id),
    fields          TEXT NOT NULL CHECK (json_valid(fields)),
    PRIMARY KEY (scheduler_epoch, seq)
) WITHOUT ROWID;

CREATE TRIGGER trg_event_no_update
BEFORE UPDATE ON event
BEGIN
    SELECT RAISE(ABORT, 'event history is append-only');
END;

CREATE TRIGGER trg_event_no_delete
BEFORE DELETE ON event
BEGIN
    SELECT RAISE(ABORT, 'event history is append-only');
END;
