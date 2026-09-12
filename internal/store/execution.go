package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/HeaInSeo/agent-control-plane/internal/domain"
	"github.com/HeaInSeo/agent-control-plane/internal/ids"
	"github.com/HeaInSeo/agent-control-plane/internal/state"
)

// ErrCompletionNotDerivable is returned when COMPLETED is requested without
// evidence that derives it.
var ErrCompletionNotDerivable = errors.New("task completion is not derivable")

// ---------------------------------------------------------------------------
// TaskRun
// ---------------------------------------------------------------------------

// CreateTaskRun stores a new scheduling subject.
func (t *Tx) CreateTaskRun(ctx context.Context, run domain.TaskRun) error {
	if err := run.Validate(); err != nil {
		return err
	}
	if run.Status == state.TaskCompleted {
		return fmt.Errorf("%w: a task cannot be created already COMPLETED", ErrCompletionNotDerivable)
	}
	now := t.Now()
	if run.CreatedAt.IsZero() {
		run.CreatedAt = now
	}
	if run.UpdatedAt.IsZero() {
		run.UpdatedAt = now
	}
	if _, err := t.tx.ExecContext(ctx,
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
	if err := status.Validate(); err != nil {
		return err
	}
	if status == state.TaskCompleted {
		return fmt.Errorf("%w: COMPLETED must be derived from evidence via CompleteTaskRunFromEvidence, "+
			"not written directly (worker exit is not completion)", ErrCompletionNotDerivable)
	}
	return t.exactlyOne(ctx, "task run", string(id),
		`UPDATE task_run SET status = ?, updated_at = ? WHERE task_id = ?`,
		string(status), formatTime(t.Now()), string(id))
}

// SetTaskCurrentAttempt points a task at the attempt that currently owns it.
func (t *Tx) SetTaskCurrentAttempt(ctx context.Context, id ids.TaskID, attempt ids.AttemptID) error {
	if err := attempt.Validate(); err != nil {
		return err
	}
	return t.exactlyOne(ctx, "task run", string(id),
		`UPDATE task_run SET current_attempt_id = ?, updated_at = ? WHERE task_id = ?`,
		string(attempt), formatTime(t.Now()), string(id))
}

// CompleteTaskRunFromEvidence is the only way a task reaches COMPLETED.
//
// It reloads the evidence, the attempt identity and the publication status
// from the database and re-derives completion through the domain rule, so the
// caller cannot supply a favourable-looking conclusion. Every failure leaves
// the task where it was.
func (t *Tx) CompleteTaskRunFromEvidence(ctx context.Context, taskID ids.TaskID, evidenceID ids.EvidenceID) error {
	run, err := t.TaskRun(ctx, taskID)
	if err != nil {
		return err
	}
	ev, err := t.Evidence(ctx, evidenceID)
	if err != nil {
		return err
	}
	attempt, err := t.WorkerAttempt(ctx, ev.AttemptID)
	if err != nil {
		return err
	}
	workspace, err := t.WorkspaceForAttempt(ctx, attempt.AttemptID)
	if err != nil {
		return err
	}

	publishStatus := state.PublishUnknown
	if ev.PublishAttemptID != nil {
		pub, err := t.PublishAttempt(ctx, *ev.PublishAttemptID)
		if err != nil {
			return err
		}
		publishStatus = pub.Status
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
	if attempt.TaskID != run.TaskID {
		return fmt.Errorf("%w: evidence attempt belongs to task %s, not %s",
			ErrCompletionNotDerivable, string(attempt.TaskID), string(run.TaskID))
	}

	return t.exactlyOne(ctx, "task run", string(taskID),
		`UPDATE task_run SET status = ?, completed_evidence_id = ?, updated_at = ? WHERE task_id = ?`,
		string(derived), string(evidenceID), formatTime(t.Now()), string(taskID))
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
	if err := a.Validate(); err != nil {
		return err
	}
	if err := t.RequireCurrentEpoch(ctx, a.SchedulerEpoch); err != nil {
		return err
	}
	now := t.Now()
	if a.CreatedAt.IsZero() {
		a.CreatedAt = now
	}
	if a.UpdatedAt.IsZero() {
		a.UpdatedAt = now
	}
	if _, err := t.tx.ExecContext(ctx,
		`INSERT INTO worker_attempt (attempt_id, task_id, packet_id, repository_subject_id,
		                             lane, intent, scheduler_epoch, fence_epoch, status,
		                             lease_expires_at, last_checkpoint_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(a.AttemptID), string(a.TaskID), string(a.PacketID), string(a.RepositorySubjectID),
		string(a.Lane), string(a.Intent), int64(a.SchedulerEpoch), int64(a.FenceEpoch),
		string(a.Status), formatTimePtr(a.LeaseExpiresAt), formatTimePtr(a.LastCheckpointAt),
		formatTime(a.CreatedAt), formatTime(a.UpdatedAt),
	); err != nil {
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

// SetWorkerAttemptStatus moves an attempt within WorkerAttemptStatus.
func (t *Tx) SetWorkerAttemptStatus(ctx context.Context, id ids.AttemptID, status state.WorkerAttemptStatus) error {
	if err := status.Validate(); err != nil {
		return err
	}
	return t.exactlyOne(ctx, "worker attempt", string(id),
		`UPDATE worker_attempt SET status = ?, updated_at = ? WHERE attempt_id = ?`,
		string(status), formatTime(t.Now()), string(id))
}

// ---------------------------------------------------------------------------
// Workspace (CC3)
// ---------------------------------------------------------------------------

// CreateWorkspace records the workspace owned by one attempt.
func (t *Tx) CreateWorkspace(ctx context.Context, w domain.Workspace) error {
	if err := w.Validate(); err != nil {
		return err
	}
	if w.CreatedAt.IsZero() {
		w.CreatedAt = t.Now()
	}
	if _, err := t.tx.ExecContext(ctx,
		`INSERT INTO workspace (workspace_id, attempt_id, task_id, repository_subject_id,
		                        base_sha, isolation_kind, root_path, created_at, released_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		string(w.WorkspaceID), string(w.AttemptID), string(w.TaskID), string(w.RepositorySubjectID),
		string(w.BaseSHA), string(w.IsolationKind), w.RootPath, formatTime(w.CreatedAt),
	); err != nil {
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
func (t *Tx) ReleaseWorkspace(ctx context.Context, id ids.WorkspaceID) error {
	return t.exactlyOne(ctx, "workspace", string(id),
		`UPDATE workspace SET released_at = ? WHERE workspace_id = ?`,
		formatTime(t.Now()), string(id))
}
