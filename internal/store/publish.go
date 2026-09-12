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

// ErrDuplicatePublishIdentity is returned when a publication intent with the
// same idempotency identity already exists.
var ErrDuplicatePublishIdentity = errors.New("duplicate publish identity")

// ---------------------------------------------------------------------------
// PublishAttempt (CC1)
// ---------------------------------------------------------------------------

// RecordPublishAttempt stores a publication intent bound to an exact immutable
// commit.
//
// M0 records intent only. No push, no GitHub API mutation and no credential
// handling exists in this milestone; the publisher that consumes these rows is
// later work. Recording the binding now means the future publisher cannot be
// written without one.
func (t *Tx) RecordPublishAttempt(ctx context.Context, p domain.PublishAttempt) error {
	if p.IdempotencyKey == "" {
		p.IdempotencyKey = domain.DeriveIdempotencyKey(p)
	}
	if err := p.Validate(); err != nil {
		return err
	}
	if err := t.RequireCurrentEpoch(ctx, p.SchedulerEpoch); err != nil {
		return err
	}
	if p.CreatedAt.IsZero() {
		p.CreatedAt = t.Now()
	}

	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO publish_attempt (publish_attempt_id, task_id, attempt_id,
		                              scheduler_epoch, fence_epoch, workspace_id,
		                              repository_subject_id, base_sha, source_commit_sha,
		                              target_ref, idempotency_key, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(p.PublishAttemptID), string(p.TaskID), string(p.AttemptID),
		int64(p.SchedulerEpoch), int64(p.FenceEpoch), string(p.WorkspaceID),
		string(p.RepositorySubjectID), string(p.BaseSHA), string(p.SourceCommitSHA),
		p.TargetRef, p.IdempotencyKey, string(p.Status), formatTime(p.CreatedAt))
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: idempotency_key %s is already recorded: %w",
				ErrDuplicatePublishIdentity, p.IdempotencyKey, err)
		}
		return fmt.Errorf("insert publish attempt: %w", err)
	}
	return nil
}

// PublishAttempt loads a publication intent.
func (t *Tx) PublishAttempt(ctx context.Context, id ids.PublishAttemptID) (domain.PublishAttempt, error) {
	var (
		out                                   domain.PublishAttempt
		pubID, taskID, attemptID, workspaceID string
		repoSubject, baseSHA, sourceSHA       string
		targetRef, idemKey, status, createdAt string
		schedEpoch, fenceEpoch                int64
	)
	err := t.tx.QueryRowContext(ctx,
		`SELECT publish_attempt_id, task_id, attempt_id, scheduler_epoch, fence_epoch,
		        workspace_id, repository_subject_id, base_sha, source_commit_sha,
		        target_ref, idempotency_key, status, created_at
		   FROM publish_attempt WHERE publish_attempt_id = ?`, string(id),
	).Scan(&pubID, &taskID, &attemptID, &schedEpoch, &fenceEpoch,
		&workspaceID, &repoSubject, &baseSHA, &sourceSHA,
		&targetRef, &idemKey, &status, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("%w: publish attempt %s", ErrNotFound, string(id))
	}
	if err != nil {
		return out, fmt.Errorf("read publish attempt: %w", err)
	}
	created, err := parseTime(createdAt)
	if err != nil {
		return out, err
	}
	return domain.PublishAttempt{
		PublishAttemptID:    ids.PublishAttemptID(pubID),
		TaskID:              ids.TaskID(taskID),
		AttemptID:           ids.AttemptID(attemptID),
		SchedulerEpoch:      domain.Epoch(schedEpoch),
		FenceEpoch:          domain.FenceEpoch(fenceEpoch),
		WorkspaceID:         ids.WorkspaceID(workspaceID),
		RepositorySubjectID: ids.RepositorySubjectID(repoSubject),
		BaseSHA:             domain.CommitSHA(baseSHA),
		SourceCommitSHA:     domain.CommitSHA(sourceSHA),
		TargetRef:           targetRef,
		IdempotencyKey:      idemKey,
		Status:              state.PublishStatus(status),
		CreatedAt:           created,
	}, nil
}

// SetPublishStatus moves a publication within PublishStatus.
func (t *Tx) SetPublishStatus(ctx context.Context, id ids.PublishAttemptID, status state.PublishStatus) error {
	if err := status.Validate(); err != nil {
		return err
	}
	return t.exactlyOne(ctx, "publish attempt", string(id),
		`UPDATE publish_attempt SET status = ? WHERE publish_attempt_id = ?`,
		string(status), string(id))
}

// ---------------------------------------------------------------------------
// EvidenceObservation (CC7)
// ---------------------------------------------------------------------------

// RecordEvidence stores an externally observed repository effect.
//
// The observation's identity fields are checked against the attempt it names
// before the insert, and again by a schema trigger. Evidence about one attempt
// can therefore never be filed under another.
func (t *Tx) RecordEvidence(ctx context.Context, e domain.EvidenceObservation) error {
	if err := e.Validate(); err != nil {
		return err
	}
	attempt, err := t.WorkerAttempt(ctx, e.AttemptID)
	if err != nil {
		return err
	}
	workspace, err := t.WorkspaceForAttempt(ctx, e.AttemptID)
	if err != nil {
		return err
	}
	if err := e.CheckAttribution(domain.AttemptIdentity{
		AttemptID:           attempt.AttemptID,
		TaskID:              attempt.TaskID,
		SchedulerEpoch:      attempt.SchedulerEpoch,
		FenceEpoch:          attempt.FenceEpoch,
		RepositorySubjectID: attempt.RepositorySubjectID,
		WorkspaceID:         workspace.WorkspaceID,
	}); err != nil {
		return err
	}

	if _, err := t.tx.ExecContext(ctx,
		`INSERT INTO evidence_observation (evidence_id, task_id, attempt_id, scheduler_epoch,
		                                   fence_epoch, repository_subject_id, workspace_id,
		                                   publish_attempt_id, published_sha, observed_sha,
		                                   observed_at, evidence_kind)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(e.EvidenceID), string(e.TaskID), string(e.AttemptID), int64(e.SchedulerEpoch),
		int64(e.FenceEpoch), string(e.RepositorySubjectID), string(e.WorkspaceID),
		publishIDArg(e.PublishAttemptID), string(e.PublishedSHA), string(e.ObservedSHA),
		formatTime(e.ObservedAt), string(e.EvidenceKind),
	); err != nil {
		return fmt.Errorf("insert evidence observation: %w", err)
	}
	return nil
}

func publishIDArg(id *ids.PublishAttemptID) any {
	if id == nil {
		return nil
	}
	return string(*id)
}

// Evidence loads an observation.
func (t *Tx) Evidence(ctx context.Context, id ids.EvidenceID) (domain.EvidenceObservation, error) {
	var (
		out                                   domain.EvidenceObservation
		evID, taskID, attemptID               string
		repoSubject, workspaceID              string
		publishedSHA, observedSHA, observedAt string
		evidenceKind                          string
		schedEpoch, fenceEpoch                int64
		publishID                             sql.NullString
	)
	err := t.tx.QueryRowContext(ctx,
		`SELECT evidence_id, task_id, attempt_id, scheduler_epoch, fence_epoch,
		        repository_subject_id, workspace_id, publish_attempt_id,
		        published_sha, observed_sha, observed_at, evidence_kind
		   FROM evidence_observation WHERE evidence_id = ?`, string(id),
	).Scan(&evID, &taskID, &attemptID, &schedEpoch, &fenceEpoch,
		&repoSubject, &workspaceID, &publishID,
		&publishedSHA, &observedSHA, &observedAt, &evidenceKind)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fmt.Errorf("%w: evidence observation %s", ErrNotFound, string(id))
	}
	if err != nil {
		return out, fmt.Errorf("read evidence observation: %w", err)
	}
	at, err := parseTime(observedAt)
	if err != nil {
		return out, err
	}
	out = domain.EvidenceObservation{
		EvidenceID:          ids.EvidenceID(evID),
		TaskID:              ids.TaskID(taskID),
		AttemptID:           ids.AttemptID(attemptID),
		SchedulerEpoch:      domain.Epoch(schedEpoch),
		FenceEpoch:          domain.FenceEpoch(fenceEpoch),
		RepositorySubjectID: ids.RepositorySubjectID(repoSubject),
		WorkspaceID:         ids.WorkspaceID(workspaceID),
		PublishedSHA:        domain.CommitSHA(publishedSHA),
		ObservedSHA:         domain.CommitSHA(observedSHA),
		ObservedAt:          at,
		EvidenceKind:        domain.EvidenceKind(evidenceKind),
	}
	if publishID.Valid {
		pid := ids.PublishAttemptID(publishID.String)
		out.PublishAttemptID = &pid
	}
	return out, nil
}
