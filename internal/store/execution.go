package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
)

// ErrCompletionNotDerivable is returned when COMPLETED is requested without
// evidence that derives it.
var ErrCompletionNotDerivable = errors.New("task completion is not derivable")

// ErrInvalidTaskRun is returned when a task run mutation is incoherent.
var ErrInvalidTaskRun = errors.New("invalid task run mutation")

// ErrForbiddenTaskTransition is returned when a task status change is not a
// permitted transition.
var ErrForbiddenTaskTransition = errors.New("forbidden task run status transition")

// ErrTaskNotLive is returned when durable state is written for a task that
// has already finished.
//
// A withdrawn or completed task takes on nothing new. Publication is the case
// that matters most — it is the irreversible step, and a cancelled task whose
// push still went out is the worst outcome this control plane can produce —
// but the same holds for workspaces and observations, which are undeletable
// and would leave permanent state belonging to work that is over.
var ErrTaskNotLive = errors.New("task is not live")

// requireLiveTask rejects durable writes against a finished task.
func (t *Tx) requireLiveTask(ctx context.Context, id ids.TaskID) error {
	run, err := t.TaskRun(ctx, id)
	if err != nil {
		return err
	}
	if run.Status.IsTerminal() {
		return fmt.Errorf("%w: task %s is %s", ErrTaskNotLive, string(id), string(run.Status))
	}
	return nil
}

// ErrFutureTimestamp is returned when a caller supplies a creation time that
// is ahead of the store's clock.
//
// A future-dated creation time is uncorrectable by design: monotonicNow
// clamps every later update up to it, so the field that records when a row
// last changed reports the wrong instant until real time catches up — and
// these rows are immutable or undeletable. ApprovePacket rejects future
// dating for the same reason.
var ErrFutureTimestamp = errors.New("timestamp is ahead of the store clock")

// requireNotFuture rejects a caller-supplied timestamp ahead of the store's
// clock. field names the column, so the error says which one.
func (t *Tx) requireNotFuture(what, field string, supplied time.Time) error {
	if now := t.Now(); supplied.After(now) {
		return fmt.Errorf("%w: %s %s %s is after now %s",
			ErrFutureTimestamp, what, field, supplied.UTC(), now.UTC())
	}
	return nil
}

// ErrModifyingSlotBusy is returned when a repository subject already has a
// live modifying attempt (CC9).
//
// This is an ordinary scheduling outcome — "come back later", not a fault —
// so it has its own sentinel. Leaving it as a raw unique-constraint error
// would make the single most common legitimate refusal indistinguishable from
// a broken database.
var ErrModifyingSlotBusy = errors.New("repository already has a live modifying attempt")

// ErrWorkspaceConflict is returned when a workspace would collide with an
// existing one, or is incoherent with the attempt that would own it.
var ErrWorkspaceConflict = errors.New("workspace conflicts with an existing workspace")

// requireUnnestedWorkspacePath rejects a path that contains, or is contained
// by, an existing workspace.
//
// Checked here as well as by trigger so the refusal is typed. The comparison
// is exact-string on the "<path>/" prefix rather than a pattern match,
// because a legitimate path may contain GLOB or LIKE metacharacters.
func (t *Tx) requireUnnestedWorkspacePath(ctx context.Context, path string) error {
	// Both directions are index-backed, so this does not degrade as the
	// workspace table grows — and it never shrinks, since workspaces reject
	// DELETE.
	//
	// "path is inside an existing workspace" is an equality test against each
	// of path's ancestors, of which there are only as many as the path has
	// components, and root_path is UNIQUE.
	//
	// "an existing workspace is inside path" is a range scan on the same
	// index: every descendant of path sorts between "path/" and "path0",
	// because '0' is the byte after '/'.
	ancestors := pathAncestors(path)
	args := make([]any, 0, len(ancestors)+2)
	placeholders := make([]string, 0, len(ancestors))
	for _, ancestor := range ancestors {
		placeholders = append(placeholders, "?")
		args = append(args, ancestor)
	}
	args = append(args, path+"/", path+"0")

	query := `SELECT root_path FROM workspace WHERE root_path > ? AND root_path < ? LIMIT 1`
	if len(ancestors) > 0 {
		query = `SELECT root_path FROM workspace
		          WHERE root_path IN (` + strings.Join(placeholders, ", ") + `)
		             OR (root_path > ? AND root_path < ?)
		          LIMIT 1`
	}

	var existing string
	err := t.tx.QueryRowContext(ctx, query, args...).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check workspace nesting: %w", err)
	}
	return fmt.Errorf("%w: %s nests with existing workspace %s",
		ErrWorkspaceConflict, path, existing)
}

// pathAncestors returns the proper ancestor directories of an absolute,
// canonical path, from the root down. The path itself is excluded: an exact
// duplicate is already caught by UNIQUE(root_path).
func pathAncestors(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	segments := strings.Split(trimmed, "/")
	ancestors := make([]string, 0, len(segments)-1)
	for i := 1; i < len(segments); i++ {
		ancestors = append(ancestors, "/"+strings.Join(segments[:i], "/"))
	}
	return ancestors
}

// isolationFor reports the isolation kind an attempt of this intent requires.
//
// CC3's rule is a fresh isolated clone per modifying attempt, so the kind
// follows from the intent rather than being chosen freely.
func isolationFor(intent state.Intent) domain.IsolationKind {
	if intent == state.IntentModifying {
		return domain.IsolationIsolatedClone
	}
	return domain.IsolationReadOnlyCheckout
}

// ---------------------------------------------------------------------------
// TaskRun
// ---------------------------------------------------------------------------

// CreateTaskRun stores a new scheduling subject.
func (t *Tx) CreateTaskRun(ctx context.Context, run domain.TaskRun) error {
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	// Default before validating, so the ordering check sees real values. The
	// validator skips the comparison when either timestamp is zero, so
	// validating first let a caller-supplied created_at pair with a
	// store-supplied updated_at that precedes it — surfacing as a raw CHECK
	// failure from the driver instead of a typed refusal.
	now := t.Now()
	if run.CreatedAt.IsZero() {
		run.CreatedAt = now
	}
	if run.UpdatedAt.IsZero() {
		run.UpdatedAt = run.CreatedAt
	}
	if err := t.requireNotFuture("task run", "created_at", run.CreatedAt); err != nil {
		return err
	}
	// The second timestamp too. monotonicNow clamps every later update up to
	// whatever updated_at holds, so a future-dated one leaves the row
	// claiming it changed in the future — and task_run rejects DELETE with
	// created_at frozen, so that claim is uncorrectable.
	if err := t.requireNotFuture("task run", "updated_at", run.UpdatedAt); err != nil {
		return err
	}
	if err := run.Validate(); err != nil {
		return err
	}
	if run.Status == state.TaskCompleted {
		return fmt.Errorf("%w: a task cannot be created already COMPLETED", ErrCompletionNotDerivable)
	}
	// Nor in any other terminal state. A task created ABANDONED can never
	// admit an attempt, can never change status, and can never be deleted —
	// a row that is dead on arrival and permanent.
	if run.Status.IsTerminal() {
		return fmt.Errorf("%w: a task cannot be created already %s",
			ErrInvalidTaskRun, string(run.Status))
	}
	// A task cannot be created already pointing at an attempt: an attempt
	// references its task, so the task has to exist first. Silently dropping
	// the field would leave the caller believing it had been stored.
	if run.CurrentAttemptID != nil {
		return fmt.Errorf("%w: a task cannot be created with a current attempt; "+
			"create the attempt and then call SetTaskCurrentAttempt", ErrInvalidTaskRun)
	}
	// The packet must still grant authority. packet_id is immutable once the
	// task exists, so a task bound to a stale or expired packet could never
	// admit an attempt, never complete, and only ever be abandoned.
	if err := t.requirePacketAuthority(ctx, run.PacketID); err != nil {
		return err
	}
	if _, err := t.exec(ctx,
		`INSERT INTO task_run (task_id, repository_subject_id, packet_id, lane, intent,
		                       status, current_attempt_id, completed_evidence_id, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)`,
		string(run.TaskID), string(run.RepositorySubjectID), string(run.PacketID),
		string(run.Lane), string(run.Intent), string(run.Status),
		formatTime(run.CreatedAt), formatTime(run.UpdatedAt),
	); err != nil {
		return fmt.Errorf("insert task run: %w", err)
	}
	return nil
}

// TaskRun loads a scheduling subject.
func (t *Tx) TaskRun(ctx context.Context, id ids.TaskID) (domain.TaskRun, error) {
	var (
		out                              domain.TaskRun
		taskID, repoSubject, packetID    string
		lane, intent, status             string
		createdAt, updatedAt             string
		currentAttempt, completedEvidenc sql.NullString
	)
	err := t.tx.QueryRowContext(ctx,
		`SELECT task_id, repository_subject_id, packet_id, lane, intent, status,
		        current_attempt_id, completed_evidence_id, created_at, updated_at
		   FROM task_run WHERE task_id = ?`, string(id),
	).Scan(&taskID, &repoSubject, &packetID, &lane, &intent, &status,
		&currentAttempt, &completedEvidenc, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("%w: task run %s", ErrNotFound, string(id))
	}
	if err != nil {
		return out, fmt.Errorf("read task run: %w", err)
	}

	created, err := parseTime(createdAt)
	if err != nil {
		return out, err
	}
	updated, err := parseTime(updatedAt)
	if err != nil {
		return out, err
	}
	out = domain.TaskRun{
		TaskID:              ids.TaskID(taskID),
		RepositorySubjectID: ids.RepositorySubjectID(repoSubject),
		PacketID:            ids.PacketID(packetID),
		Lane:                state.Lane(lane),
		Intent:              state.Intent(intent),
		Status:              state.TaskRunStatus(status),
		CreatedAt:           created,
		UpdatedAt:           updated,
	}
	if currentAttempt.Valid {
		id := ids.AttemptID(currentAttempt.String)
		out.CurrentAttemptID = &id
	}
	if completedEvidenc.Valid {
		id := ids.EvidenceID(completedEvidenc.String)
		out.CompletedEvidenceID = &id
	}
	return out, nil
}

// SetTaskRunStatus moves a task within TaskRunStatus.
//
// COMPLETED is deliberately not reachable through this method. Completion is
// derived from evidence by CompleteTaskRunFromEvidence, so no caller — and in
// particular no worker-exit handler — can write it directly (I1).
func (t *Tx) SetTaskRunStatus(ctx context.Context, id ids.TaskID, status state.TaskRunStatus) error {
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	if err := status.Validate(); err != nil {
		return err
	}
	if status == state.TaskCompleted {
		return fmt.Errorf("%w: COMPLETED must be derived from evidence via CompleteTaskRunFromEvidence, "+
			"not written directly (worker exit is not completion)", ErrCompletionNotDerivable)
	}
	current, err := t.TaskRun(ctx, id)
	if err != nil {
		return err
	}
	if !current.Status.CanTransitionTo(status) {
		return fmt.Errorf("%w: task %s cannot move from %q to %q",
			ErrForbiddenTaskTransition, string(id), string(current.Status), string(status))
	}
	// Re-asserting the current status is a no-op, not a move. Without this a
	// long-finished task could be rewritten to look freshly touched — the one
	// durable write that would still accept a task everything else refuses.
	if status == current.Status {
		return nil
	}
	return t.exactlyOne(ctx, "task run", string(id),
		`UPDATE task_run SET status = ?, updated_at = ? WHERE task_id = ?`,
		string(status), formatTime(t.monotonicNow(current.UpdatedAt)), string(id))
}

// SetTaskCurrentAttempt points a task at the attempt that currently owns it.
//
// The pointer only ever moves forward, to a live attempt. Completion is
// expressed as "the evidence came from the task's current attempt", so a
// pointer that could be moved backwards onto a fenced-out attempt would
// quietly undo that guarantee.
func (t *Tx) SetTaskCurrentAttempt(ctx context.Context, id ids.TaskID, attempt ids.AttemptID) error {
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	if err := attempt.Validate(); err != nil {
		return err
	}
	candidate, err := t.WorkerAttempt(ctx, attempt)
	if err != nil {
		return err
	}
	// Pointing a task at an attempt from a retired generation wedges it: the
	// attempt holds the repository's modifying slot so no replacement can be
	// admitted, while evidence against it is refused for the stale epoch, so
	// nothing can complete either.
	if err := t.RequireCurrentEpoch(ctx, candidate.SchedulerEpoch); err != nil {
		return err
	}
	if candidate.TaskID != id {
		return fmt.Errorf("%w: attempt %s belongs to task %s, not %s",
			ErrInvalidTaskRun, string(attempt), string(candidate.TaskID), string(id))
	}
	if candidate.Status.IsTerminal() {
		return fmt.Errorf("%w: attempt %s is terminal (%s) and cannot own a task",
			ErrInvalidTaskRun, string(attempt), string(candidate.Status))
	}

	run, err := t.TaskRun(ctx, id)
	if err != nil {
		return err
	}
	// A finished task does not take on new work. Letting it would occupy the
	// repository's single modifying slot on behalf of a task that is over,
	// and would move current_attempt_id away from the attempt whose evidence
	// established completion.
	if run.Status.IsTerminal() {
		return fmt.Errorf("%w: task %s is %s and cannot take a new current attempt",
			ErrInvalidTaskRun, string(id), string(run.Status))
	}
	if run.CurrentAttemptID != nil {
		previous, err := t.WorkerAttempt(ctx, *run.CurrentAttemptID)
		if err != nil {
			return err
		}
		if candidate.FenceEpoch < previous.FenceEpoch {
			return fmt.Errorf("%w: task %s is at fence epoch %d; attempt %s is at %d",
				ErrInvalidTaskRun, string(id), int64(previous.FenceEpoch),
				string(attempt), int64(candidate.FenceEpoch))
		}
	}
	// Re-asserting the attempt a task already points at is a no-op, as for
	// every status setter. The forward-only trigger is guarded on the value
	// changing, so without this the write would land and rewrite updated_at.
	if run.CurrentAttemptID != nil && *run.CurrentAttemptID == attempt {
		return nil
	}
	return t.exactlyOne(ctx, "task run", string(id),
		`UPDATE task_run SET current_attempt_id = ?, updated_at = ? WHERE task_id = ?`,
		string(attempt), formatTime(t.monotonicNow(run.UpdatedAt)), string(id))
}

// CompleteTaskRunFromEvidence is the only way a task reaches COMPLETED.
//
// It reloads the evidence, the attempt identity and the publication status
// from the database and re-derives completion through the domain rule, so the
// caller cannot supply a favourable-looking conclusion. Every failure leaves
// the task where it was.
func (t *Tx) CompleteTaskRunFromEvidence(ctx context.Context, taskID ids.TaskID, evidenceID ids.EvidenceID) error {
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	run, err := t.TaskRun(ctx, taskID)
	if err != nil {
		return err
	}
	// Completion is idempotent for the evidence that established it, and
	// refused for any other: rebinding a completed task to different
	// evidence would lose which observation actually established it.
	if run.Status == state.TaskCompleted {
		if run.CompletedEvidenceID != nil && *run.CompletedEvidenceID == evidenceID {
			return nil
		}
		return fmt.Errorf("%w: task %s is already COMPLETED on other evidence",
			ErrCompletionNotDerivable, string(taskID))
	}
	ev, err := t.Evidence(ctx, evidenceID)
	if err != nil {
		return err
	}
	attempt, err := t.WorkerAttempt(ctx, ev.AttemptID)
	if err != nil {
		return err
	}
	if attempt.TaskID != run.TaskID {
		return fmt.Errorf("%w: evidence attempt belongs to task %s, not %s",
			ErrCompletionNotDerivable, string(attempt.TaskID), string(run.TaskID))
	}
	// The evidence must come from the attempt that currently owns the task.
	// Without this, a fenced-out attempt's evidence completes a task that a
	// successor is still actively running — the publication path already
	// revalidates both of these, and completion was the asymmetric hole.
	if run.CurrentAttemptID == nil || *run.CurrentAttemptID != attempt.AttemptID {
		current := "none"
		if run.CurrentAttemptID != nil {
			current = string(*run.CurrentAttemptID)
		}
		return fmt.Errorf("%w: task %s current attempt is %s, evidence is from %s",
			ErrCompletionNotDerivable, string(run.TaskID), current, string(attempt.AttemptID))
	}
	if attempt.Status.IsTerminal() {
		return fmt.Errorf("%w: attempt %s is terminal (%s)",
			ErrCompletionNotDerivable, string(attempt.AttemptID), string(attempt.Status))
	}
	// Driving a task to COMPLETED is an exercise of scheduler ownership, so
	// it binds the current generation exactly as admitting an attempt and
	// recording a publication do. An attempt from a retired epoch stays
	// readable; it does not get to complete anything. Reconciling work that
	// straddles a scheduler restart is crash-recovery, a later milestone.
	if err := t.RequireCurrentEpoch(ctx, attempt.SchedulerEpoch); err != nil {
		return err
	}
	workspace, err := t.WorkspaceForAttempt(ctx, attempt.AttemptID)
	if err != nil {
		return err
	}
	if workspace.ReleasedAt != nil {
		return fmt.Errorf("%w: workspace %s is released",
			ErrCompletionNotDerivable, string(workspace.WorkspaceID))
	}

	publishStatus := state.PublishUnknown
	if ev.PublishAttemptID != nil {
		pub, err := t.PublishAttempt(ctx, *ev.PublishAttemptID)
		if err != nil {
			return err
		}
		publishStatus = pub.Status
	}

	// Refuse a task whose scheduling state cannot legally reach COMPLETED,
	// with a typed error. Without this the schema trigger rejects the UPDATE
	// and the caller sees a raw driver constraint failure for what is in fact
	// a legitimate refusal.
	if !run.Status.CanTransitionTo(state.TaskCompleted) {
		return fmt.Errorf("%w: task %s is %s and cannot reach COMPLETED",
			ErrForbiddenTaskTransition, string(taskID), string(run.Status))
	}

	derived, err := domain.DeriveTaskCompletion(domain.CompletionInput{
		Attempt: domain.AttemptIdentity{
			AttemptID:           attempt.AttemptID,
			TaskID:              attempt.TaskID,
			SchedulerEpoch:      attempt.SchedulerEpoch,
			FenceEpoch:          attempt.FenceEpoch,
			RepositorySubjectID: attempt.RepositorySubjectID,
			WorkspaceID:         workspace.WorkspaceID,
		},
		Evidence:      ev,
		PublishStatus: publishStatus,
		Intent:        run.Intent,
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCompletionNotDerivable, err)
	}

	return t.exactlyOne(ctx, "task run", string(taskID),
		`UPDATE task_run SET status = ?, completed_evidence_id = ?, updated_at = ? WHERE task_id = ?`,
		string(derived), string(evidenceID),
		formatTime(t.monotonicNow(run.UpdatedAt)), string(taskID))
}

// ---------------------------------------------------------------------------
// WorkerAttempt
// ---------------------------------------------------------------------------

// NextFenceEpoch returns the next fencing token for a task.
func (t *Tx) NextFenceEpoch(ctx context.Context, taskID ids.TaskID) (domain.FenceEpoch, error) {
	var current int64
	if err := t.tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(fence_epoch), 0) FROM worker_attempt WHERE task_id = ?`, string(taskID),
	).Scan(&current); err != nil {
		return 0, fmt.Errorf("read fence epoch: %w", err)
	}
	return domain.FenceEpoch(current + 1), nil
}

// CreateWorkerAttempt admits one bounded execution.
//
// The attempt must bind the current scheduler epoch. The check is made here
// and again by a schema trigger, because this is the boundary where a stale
// scheduler generation would otherwise acquire live ownership.
func (t *Tx) CreateWorkerAttempt(ctx context.Context, a domain.WorkerAttempt) error {
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	now := t.Now()
	if a.CreatedAt.IsZero() {
		a.CreatedAt = now
	}
	if a.UpdatedAt.IsZero() {
		a.UpdatedAt = a.CreatedAt
	}
	if err := t.requireNotFuture("worker attempt", "created_at", a.CreatedAt); err != nil {
		return err
	}
	if err := t.requireNotFuture("worker attempt", "updated_at", a.UpdatedAt); err != nil {
		return err
	}
	// A lease already expired against real time is the hazard the entity
	// guard names, and comparing it to created_at does not catch it: a
	// created_at an hour in the past admits a lease half an hour in the
	// past. Nothing writes this column after insert and worker_attempt
	// rejects DELETE, so such an attempt could never be corrected.
	if a.LeaseExpiresAt != nil {
		if now := t.Now(); a.LeaseExpiresAt.Before(now) {
			return fmt.Errorf("%w: worker attempt lease_expires_at %s has already passed (now %s)",
				domain.ErrInvalidEntity, a.LeaseExpiresAt.UTC(), now.UTC())
		}
	}
	if a.LastCheckpointAt != nil {
		if err := t.requireNotFuture("worker attempt", "last_checkpoint_at", *a.LastCheckpointAt); err != nil {
			return err
		}
	}
	if err := a.Validate(); err != nil {
		return err
	}
	if err := t.RequireCurrentEpoch(ctx, a.SchedulerEpoch); err != nil {
		return err
	}
	// An attempt is created live. A terminal one is dead on arrival and
	// permanent: it can never transition, nothing will accept it, and it has
	// already burned a fence epoch for its task.
	if a.Status.IsTerminal() {
		return fmt.Errorf("%w: an attempt cannot be created already %s",
			ErrForbiddenAttemptTransition, string(a.Status))
	}
	run, err := t.TaskRun(ctx, a.TaskID)
	if err != nil {
		return err
	}
	if run.Status.IsTerminal() {
		return fmt.Errorf("%w: task %s is %s and cannot admit a new attempt",
			ErrTaskNotLive, string(a.TaskID), string(run.Status))
	}
	// Admission is the third place packet authority has to hold, alongside
	// launch/resume and publication. A STALE or SUPERSEDED packet means
	// execution must stop, so it must not be the basis for starting more.
	packet, err := t.Packet(ctx, a.PacketID)
	if err != nil {
		return err
	}
	if !packet.Status.GrantsExecutionAuthority() {
		return fmt.Errorf("%w: packet %s status is %q",
			domain.ErrPacketNoAuthority, string(a.PacketID), string(packet.Status))
	}
	if !t.Now().Before(packet.ExpiresAt) {
		return fmt.Errorf("%w: packet %s expired at %s",
			domain.ErrPacketExpired, string(a.PacketID), packet.ExpiresAt.UTC())
	}
	if _, err := t.exec(ctx,
		`INSERT INTO worker_attempt (attempt_id, task_id, packet_id, repository_subject_id,
		                             lane, intent, scheduler_epoch, fence_epoch, status,
		                             lease_expires_at, last_checkpoint_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(a.AttemptID), string(a.TaskID), string(a.PacketID), string(a.RepositorySubjectID),
		string(a.Lane), string(a.Intent), int64(a.SchedulerEpoch), int64(a.FenceEpoch),
		string(a.Status), formatTimePtr(a.LeaseExpiresAt), formatTimePtr(a.LastCheckpointAt),
		formatTime(a.CreatedAt), formatTime(a.UpdatedAt),
	); err != nil {
		if isUniqueViolationOn(err, "worker_attempt.repository_subject_id") {
			return fmt.Errorf("%w: repository subject %s (lane-agnostic): %w",
				ErrModifyingSlotBusy, string(a.RepositorySubjectID), err)
		}
		// The monotonic trigger is BEFORE INSERT, so it aborts before the
		// UNIQUE(task_id, fence_epoch) constraint is ever evaluated — a
		// uniqueness match here could never fire. Match what actually
		// happens instead, so a reused fencing token is a typed refusal.
		if isTriggerAbort(err, "fence_epoch must be monotonic") ||
			isUniqueViolationOn(err, "worker_attempt.task_id", "worker_attempt.fence_epoch") {
			return fmt.Errorf("%w: fence epoch %d already issued for task %s: %w",
				ErrInvalidTaskRun, int64(a.FenceEpoch), string(a.TaskID), err)
		}
		return fmt.Errorf("insert worker attempt: %w", err)
	}
	return nil
}

// WorkerAttempt loads one attempt. Attempts from older epochs stay readable.
func (t *Tx) WorkerAttempt(ctx context.Context, id ids.AttemptID) (domain.WorkerAttempt, error) {
	var (
		out                                      domain.WorkerAttempt
		attemptID, taskID, packetID, repoSubject string
		lane, intent, status                     string
		schedEpoch, fenceEpoch                   int64
		createdAt, updatedAt                     string
		lease, checkpoint                        sql.NullString
	)
	err := t.tx.QueryRowContext(ctx,
		`SELECT attempt_id, task_id, packet_id, repository_subject_id, lane, intent,
		        scheduler_epoch, fence_epoch, status, lease_expires_at, last_checkpoint_at,
		        created_at, updated_at
		   FROM worker_attempt WHERE attempt_id = ?`, string(id),
	).Scan(&attemptID, &taskID, &packetID, &repoSubject, &lane, &intent,
		&schedEpoch, &fenceEpoch, &status, &lease, &checkpoint, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("%w: worker attempt %s", ErrNotFound, string(id))
	}
	if err != nil {
		return out, fmt.Errorf("read worker attempt: %w", err)
	}

	created, err := parseTime(createdAt)
	if err != nil {
		return out, err
	}
	updated, err := parseTime(updatedAt)
	if err != nil {
		return out, err
	}
	leaseAt, err := parseTimePtr(lease)
	if err != nil {
		return out, err
	}
	checkpointAt, err := parseTimePtr(checkpoint)
	if err != nil {
		return out, err
	}
	return domain.WorkerAttempt{
		AttemptID:           ids.AttemptID(attemptID),
		TaskID:              ids.TaskID(taskID),
		PacketID:            ids.PacketID(packetID),
		RepositorySubjectID: ids.RepositorySubjectID(repoSubject),
		Lane:                state.Lane(lane),
		Intent:              state.Intent(intent),
		SchedulerEpoch:      domain.Epoch(schedEpoch),
		FenceEpoch:          domain.FenceEpoch(fenceEpoch),
		Status:              state.WorkerAttemptStatus(status),
		LeaseExpiresAt:      leaseAt,
		LastCheckpointAt:    checkpointAt,
		CreatedAt:           created,
		UpdatedAt:           updated,
	}, nil
}

// ErrForbiddenAttemptTransition is returned when an attempt status change is
// not a permitted transition.
var ErrForbiddenAttemptTransition = errors.New("forbidden worker attempt status transition")

// SetWorkerAttemptStatus moves an attempt within WorkerAttemptStatus.
//
// Only a permitted transition is accepted: live states move forward and a
// terminal state is final. The same rule is enforced by a schema trigger.
func (t *Tx) SetWorkerAttemptStatus(ctx context.Context, id ids.AttemptID, status state.WorkerAttemptStatus) error {
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	if err := status.Validate(); err != nil {
		return err
	}
	current, err := t.WorkerAttempt(ctx, id)
	if err != nil {
		return err
	}
	if !current.Status.CanTransitionTo(status) {
		return fmt.Errorf("%w: attempt %s cannot move from %q to %q",
			ErrForbiddenAttemptTransition, string(id), string(current.Status), string(status))
	}
	// Re-asserting the current status is a no-op, as for publications and
	// tasks: a recovery or reconciler loop must not make updated_at stop
	// identifying when the attempt actually last changed.
	if status == current.Status {
		return nil
	}
	return t.exactlyOne(ctx, "worker attempt", string(id),
		`UPDATE worker_attempt SET status = ?, updated_at = ? WHERE attempt_id = ?`,
		string(status), formatTime(t.monotonicNow(current.UpdatedAt)), string(id))
}

// ---------------------------------------------------------------------------
// Workspace (CC3)
// ---------------------------------------------------------------------------

// CreateWorkspace records the workspace owned by one attempt.
func (t *Tx) CreateWorkspace(ctx context.Context, w domain.Workspace) error {
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	if w.CreatedAt.IsZero() {
		w.CreatedAt = t.Now()
	}
	if err := t.requireNotFuture("workspace", "created_at", w.CreatedAt); err != nil {
		return err
	}
	if err := w.Validate(); err != nil {
		return err
	}
	// A workspace is created live. Silently dropping a supplied ReleasedAt —
	// the INSERT writes NULL — would leave the caller believing it had stored
	// a released workspace while every released-workspace guard downstream
	// saw a live one.
	if w.ReleasedAt != nil {
		return fmt.Errorf("%w: a workspace cannot be created already released; "+
			"create it and then call ReleaseWorkspace", ErrWorkspaceConflict)
	}

	// The same liveness fence publication applies. A workspace row is
	// undeletable and its root_path is unique, so handing one to a terminal
	// attempt burns that directory for good on a workspace that can never be
	// published from and can never complete anything.
	attempt, err := t.WorkerAttempt(ctx, w.AttemptID)
	if err != nil {
		return err
	}
	// Checked here as well as by trigger, so a mis-wired workspace is a typed
	// refusal rather than a raw constraint failure a caller cannot tell apart
	// from a corrupt database.
	if w.TaskID != attempt.TaskID {
		return fmt.Errorf("%w: attempt %s belongs to task %s, not %s",
			ErrWorkspaceConflict, string(w.AttemptID), string(attempt.TaskID), string(w.TaskID))
	}
	if w.RepositorySubjectID != attempt.RepositorySubjectID {
		return fmt.Errorf("%w: attempt %s operates on repository subject %s, not %s",
			ErrWorkspaceConflict, string(w.AttemptID),
			string(attempt.RepositorySubjectID), string(w.RepositorySubjectID))
	}
	if want := isolationFor(attempt.Intent); w.IsolationKind != want {
		return fmt.Errorf("%w: a %s attempt requires isolation %s, not %s",
			ErrWorkspaceConflict, string(attempt.Intent), string(want), string(w.IsolationKind))
	}
	if err := t.RequireCurrentEpoch(ctx, attempt.SchedulerEpoch); err != nil {
		return err
	}
	if attempt.Status.IsTerminal() {
		return fmt.Errorf("%w: attempt %s is terminal (%s)",
			ErrWorkspaceConflict, string(w.AttemptID), string(attempt.Status))
	}
	if err := t.requireLiveTask(ctx, w.TaskID); err != nil {
		return err
	}
	// The workspace writer was the only execution-setup path without this.
	// A STALE or expired packet means execution must stop, and a workspace
	// row is undeletable with a UNIQUE root_path — so minting one under
	// lapsed authority permanently burns a directory for work that can never
	// be published from or completed, which is the same argument the
	// terminal-attempt fence above rests on.
	if err := t.requirePacketAuthority(ctx, attempt.PacketID); err != nil {
		return err
	}
	if err := t.requireUnnestedWorkspacePath(ctx, w.RootPath); err != nil {
		return err
	}
	if _, err := t.exec(ctx,
		`INSERT INTO workspace (workspace_id, attempt_id, task_id, repository_subject_id,
		                        base_sha, isolation_kind, root_path, created_at, released_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		string(w.WorkspaceID), string(w.AttemptID), string(w.TaskID), string(w.RepositorySubjectID),
		string(w.BaseSHA), string(w.IsolationKind), w.RootPath, formatTime(w.CreatedAt),
	); err != nil {
		if isUniqueViolationOn(err, "workspace.root_path") {
			return fmt.Errorf("%w: directory %s is already a workspace: %w",
				ErrWorkspaceConflict, w.RootPath, err)
		}
		if isUniqueViolationOn(err, "workspace.attempt_id") {
			return fmt.Errorf("%w: attempt %s already owns a workspace: %w",
				ErrWorkspaceConflict, string(w.AttemptID), err)
		}
		return fmt.Errorf("insert workspace: %w", err)
	}
	return nil
}

// Workspace loads a workspace by its identifier.
func (t *Tx) Workspace(ctx context.Context, id ids.WorkspaceID) (domain.Workspace, error) {
	return t.scanWorkspace(t.tx.QueryRowContext(ctx,
		`SELECT workspace_id, attempt_id, task_id, repository_subject_id, base_sha,
		        isolation_kind, root_path, created_at, released_at
		   FROM workspace WHERE workspace_id = ?`, string(id)),
		fmt.Sprintf("workspace %s", string(id)))
}

// WorkspaceForAttempt loads the workspace owned by an attempt.
//
// There is no lookup that returns another attempt's workspace: the schema
// holds one workspace per attempt and one attempt per root path.
func (t *Tx) WorkspaceForAttempt(ctx context.Context, id ids.AttemptID) (domain.Workspace, error) {
	return t.scanWorkspace(t.tx.QueryRowContext(ctx,
		`SELECT workspace_id, attempt_id, task_id, repository_subject_id, base_sha,
		        isolation_kind, root_path, created_at, released_at
		   FROM workspace WHERE attempt_id = ?`, string(id)),
		fmt.Sprintf("workspace for attempt %s", string(id)))
}

func (t *Tx) scanWorkspace(row *sql.Row, what string) (domain.Workspace, error) {
	var (
		out                                       domain.Workspace
		wsID, attemptID, taskID, repoSubject      string
		baseSHA, isolationKind, rootPath, created string
		released                                  sql.NullString
	)
	err := row.Scan(&wsID, &attemptID, &taskID, &repoSubject, &baseSHA,
		&isolationKind, &rootPath, &created, &released)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("%w: %s", ErrNotFound, what)
	}
	if err != nil {
		return out, fmt.Errorf("read %s: %w", what, err)
	}
	createdAt, err := parseTime(created)
	if err != nil {
		return out, err
	}
	releasedAt, err := parseTimePtr(released)
	if err != nil {
		return out, err
	}
	return domain.Workspace{
		WorkspaceID:         ids.WorkspaceID(wsID),
		AttemptID:           ids.AttemptID(attemptID),
		TaskID:              ids.TaskID(taskID),
		RepositorySubjectID: ids.RepositorySubjectID(repoSubject),
		BaseSHA:             domain.CommitSHA(baseSHA),
		IsolationKind:       domain.IsolationKind(isolationKind),
		RootPath:            rootPath,
		CreatedAt:           createdAt,
		ReleasedAt:          releasedAt,
	}, nil
}

// ReleaseWorkspace marks a workspace as no longer usable. A released workspace
// can never be selected for publication.
//
// Releasing an already-released workspace is a no-op rather than a second
// write: the first release is when the workspace stopped being usable, and
// overwriting that timestamp would lose it.
func (t *Tx) ReleaseWorkspace(ctx context.Context, id ids.WorkspaceID) error {
	if err := t.RequireOwnership(ctx); err != nil {
		return err
	}
	existing, err := t.Workspace(ctx, id)
	if err != nil {
		return err
	}
	if existing.ReleasedAt != nil {
		return nil
	}
	return t.exactlyOne(ctx, "workspace", string(id),
		`UPDATE workspace SET released_at = ? WHERE workspace_id = ?`,
		formatTime(t.monotonicNow(existing.CreatedAt)), string(id))
}
