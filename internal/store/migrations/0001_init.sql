-- 0001_init: M0 durable foundation of the Agent Execution Control Plane.
--
-- Invariants that can be expressed in the schema are expressed here, so that a
-- future code path cannot bypass them by forgetting a check. Where a CHECK or
-- a partial unique index is enough, no trigger is used; triggers cover
-- cross-row identity coherence, which constraints cannot express.

-- ---------------------------------------------------------------------------
-- Database identity marker.
--
-- Open() refuses a non-empty database that does not carry this marker, so the
-- control plane can never quietly adopt, migrate or overwrite somebody else's
-- SQLite file.
-- ---------------------------------------------------------------------------
CREATE TABLE control_plane_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

INSERT INTO control_plane_meta (key, value) VALUES
    ('db_kind', 'agent-control-plane'),
    ('db_contract', 'v0.1');

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

-- Source binding is what makes the packet a snapshot. Rebinding an approved
-- packet to a different design revision would silently re-authorise it.
CREATE TRIGGER trg_execution_packet_binding_immutable
BEFORE UPDATE ON execution_packet
FOR EACH ROW
WHEN NEW.source_revision <> OLD.source_revision
  OR NEW.source_digest   <> OLD.source_digest
  OR NEW.packet_digest   <> OLD.packet_digest
  OR NEW.allowed_scope   <> OLD.allowed_scope
  OR NEW.forbidden_scope <> OLD.forbidden_scope
  OR NEW.task_id         <> OLD.task_id
  OR NEW.repository_subject_id <> OLD.repository_subject_id
BEGIN
    SELECT RAISE(ABORT, 'approved packet binding and scope are immutable');
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
CREATE TRIGGER trg_task_run_completion_evidence_binding
BEFORE UPDATE ON task_run
FOR EACH ROW
WHEN NEW.completed_evidence_id IS NOT NULL
  AND (SELECT task_id FROM evidence_observation WHERE evidence_id = NEW.completed_evidence_id) IS NOT NEW.task_id
BEGIN
    SELECT RAISE(ABORT, 'completion evidence must be attributed to this task');
END;

-- A task may only point at an attempt that belongs to it.
CREATE TRIGGER trg_task_run_current_attempt_binding
BEFORE UPDATE ON task_run
FOR EACH ROW
WHEN NEW.current_attempt_id IS NOT NULL
  AND (SELECT task_id FROM worker_attempt WHERE attempt_id = NEW.current_attempt_id) IS NOT NEW.task_id
BEGIN
    SELECT RAISE(ABORT, 'current_attempt_id must belong to this task');
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
WHEN NEW.attempt_id            <> OLD.attempt_id
  OR NEW.task_id               <> OLD.task_id
  OR NEW.packet_id             <> OLD.packet_id
  OR NEW.repository_subject_id <> OLD.repository_subject_id
  OR NEW.scheduler_epoch       <> OLD.scheduler_epoch
  OR NEW.fence_epoch           <> OLD.fence_epoch
  OR NEW.intent                <> OLD.intent
BEGIN
    SELECT RAISE(ABORT, 'worker_attempt identity is immutable');
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

CREATE TRIGGER trg_workspace_ownership_immutable
BEFORE UPDATE ON workspace
FOR EACH ROW
WHEN NEW.attempt_id            <> OLD.attempt_id
  OR NEW.task_id               <> OLD.task_id
  OR NEW.repository_subject_id <> OLD.repository_subject_id
  OR NEW.base_sha              <> OLD.base_sha
  OR NEW.root_path             <> OLD.root_path
BEGIN
    SELECT RAISE(ABORT, 'workspace ownership is immutable; a workspace cannot be rebound to another attempt');
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
    -- Fully qualified, non-symbolic ref. Finer-grained ref-name rules live in
    -- domain.ValidateTargetRef; the column keeps the load-bearing part.
    -- GLOB rather than LIKE for the symbolic-ref check: SQLite's LIKE is
    -- case-insensitive, which would reject every refs/heads/... target.
    target_ref            TEXT NOT NULL CHECK (target_ref GLOB 'refs/*' AND target_ref NOT GLOB '*HEAD*' AND target_ref NOT LIKE '% %' AND target_ref NOT LIKE '%*%' AND target_ref NOT LIKE '%..%'),

    -- A retry of the same intent reproduces the same key and is rejected as a
    -- duplicate publication identity.
    idempotency_key       TEXT NOT NULL UNIQUE CHECK (length(idempotency_key) > 0),

    status                TEXT NOT NULL CHECK (status IN ('PENDING', 'APPLIED', 'OBSERVED', 'REJECTED', 'UNKNOWN')),
    created_at            TEXT NOT NULL
) WITHOUT ROWID;

CREATE INDEX ix_publish_attempt_attempt ON publish_attempt (attempt_id);

CREATE TRIGGER trg_publish_attempt_epoch_current
BEFORE INSERT ON publish_attempt
FOR EACH ROW
WHEN NEW.scheduler_epoch IS NOT (SELECT current_epoch FROM scheduler_ownership WHERE id = 1)
BEGIN
    SELECT RAISE(ABORT, 'publish_attempt must bind the current scheduler epoch');
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
  OR NEW.idempotency_key       <> OLD.idempotency_key
BEGIN
    SELECT RAISE(ABORT, 'publish_attempt identity is immutable; only status may change');
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

    observed_at           TEXT NOT NULL,
    evidence_kind         TEXT NOT NULL CHECK (evidence_kind IN ('BRANCH_HEAD', 'PULL_REQUEST_HEAD', 'READ_ONLY_REVIEW'))
) WITHOUT ROWID;

CREATE INDEX ix_evidence_attempt ON evidence_observation (attempt_id);

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

CREATE TRIGGER trg_evidence_immutable
BEFORE UPDATE ON evidence_observation
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'evidence observations are immutable');
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
    task_id         TEXT,
    attempt_id      TEXT,
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
